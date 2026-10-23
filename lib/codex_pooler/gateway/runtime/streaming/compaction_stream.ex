defmodule CodexPooler.Gateway.Runtime.Streaming.CompactionStream do
  @moduledoc false

  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Payloads.CompactionTrigger
  alias CodexPooler.Gateway.Runtime.Finalization.Metadata
  alias CodexPooler.Gateway.Runtime.Streaming.CompactionResultCollector
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.Gateway.Transports.Streaming.StreamRelay

  # Codex ignores unknown data events, but unlike SSE comments these reset its
  # parsed-event idle timer. Never forward provisional encrypted compaction items.
  @keepalive ~s(event: keepalive\ndata: {"type":"keepalive"}\n\n)

  @spec result(Req.Response.t(), map(), map()) :: map()
  def result(response, context, callbacks) do
    headers = Metadata.response_headers(response, true, context.request_options)

    %{
      status: 200,
      headers: [
        {"content-type", "text/event-stream"},
        {"cache-control", "no-cache"},
        {"x-accel-buffering", "no"}
        | Enum.reject(headers, &(elem(&1, 0) in ["content-type", "cache-control", "x-accel-buffering"]))
      ],
      deferred_stream: fn conn -> run(conn, response, context, callbacks) end
    }
  end

  defp run(conn, response, context, callbacks) do
    token = DeferredStreamRegistry.register(%{request_id: context.reserved.request.id, attempt_id: context.attempt && context.attempt.id})

    try do
      outcome =
        CompactionResultCollector.relay(response, context, callbacks, fn state, upstream, handlers ->
          state = Map.merge(state, %{conn: conn, last_keepalive_ms: now()})
          StreamRelay.run(state, upstream, live_handlers(handlers, token))
        end)

      delivered = deliver(outcome, conn)
      DeferredStreamRegistry.finish(token, if(match?({:ok, _}, outcome) and match?({:ok, _}, delivered), do: :completed, else: :failed))
      delivered
    catch
      kind, reason ->
        DeferredStreamRegistry.finish(token, :failed)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp live_handlers(handlers, token) do
    interval = OperationalSettings.current().sse_keepalive_interval_ms
    collect_chunk = handlers.write_chunk

    Map.merge(handlers, %{
      drain_token: token,
      keepalive_interval_ms: interval,
      write_keepalive: &keepalive/1,
      finalize_success: with_connection(handlers.finalize_success),
      finalize_failure: with_connection(handlers.finalize_failure),
      write_chunk: fn state, data ->
        with {:ok, state} <- collect_chunk.(state, data) do
          # Upstream progress can be frequent while *all* of it is withheld.
          # Do not let that activity postpone downstream liveness indefinitely.
          if interval > 0 and now() - state.last_keepalive_ms >= interval,
            do: keepalive(state),
            else: {:ok, state}
        end
      end
    })
  end

  # Preserve the connection on a failed settlement/validation too. Before the
  # first keepalive the controller can still send the original HTTP error.
  defp with_connection(callback) when is_function(callback, 2) do
    fn body, state -> carry_connection(callback.(body, state), state.conn) end
  end

  defp with_connection(callback) when is_function(callback, 3) do
    fn body, reason, state -> carry_connection(callback.(body, reason, state), state.conn) end
  end

  defp carry_connection({:error, error}, conn), do: {:error, {:compaction_stream, conn, error}}
  defp carry_connection(result, _conn), do: result

  defp keepalive(state) do
    conn = start_stream(state.conn)
    state = Map.merge(state, %{conn: conn, last_keepalive_ms: now(), client_response_status: 200})

    case Plug.Conn.chunk(conn, @keepalive) do
      {:ok, conn} -> {:ok, %{state | conn: conn}}
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp deliver({:ok, state}, _conn) do
    state
    |> CompactionResultCollector.result()
    |> CompactionTrigger.adapt_gateway_result()
    |> write_result(state.conn)
  end

  defp deliver({:error, {:compaction_stream, conn, error}}, _initial_conn), do: write_result({:error, error}, conn)
  defp deliver({:error, error}, conn), do: write_result({:error, error}, conn)

  # CompactionTrigger constructs JSON-encoded SSE with a fixed text/event-stream
  # content type; no provider HTML is sent as an HTML document.
  # sobelow_skip ["XSS.SendResp"]
  defp write_result({:ok, %{raw_body: body}}, %{state: state} = conn) when state != :chunked,
    do: {:ok, Plug.Conn.send_resp(conn, 200, body)}

  defp write_result({:ok, %{raw_body: body}}, conn) do
    case Plug.Conn.chunk(conn, body) do
      {:ok, conn} -> {:ok, conn}
      {:error, reason} -> {:error, reason, conn}
    end
  end

  defp write_result({:error, error}, %{state: state}) when state != :chunked, do: {:error, error}

  defp write_result({:error, error}, conn) do
    # Headers are committed: report a failed SSE turn, not a successful empty
    # compaction or a second HTTP response. Only collector-approved fields leave.
    error = %{
      "code" => error_code(error),
      "message" => "upstream compact stream failed"
    }

    event = %{"type" => "response.failed", "response" => %{"status" => "failed", "error" => error}, "error" => error}

    case Plug.Conn.chunk(conn, ["event: response.failed\ndata: ", CodexPooler.JSON.encode!(event), "\n\n"]) do
      {:ok, conn} -> {:ok, conn}
      {:error, reason} -> {:error, reason, conn}
    end
  end

  defp error_code(%{code: code}) when code in ["insufficient_quota", "invalid_compaction_response"], do: code
  defp error_code(_error), do: "upstream_stream_error"
  defp start_stream(%{state: :chunked} = conn), do: conn
  defp start_stream(conn), do: Plug.Conn.send_chunked(conn, 200)
  defp now, do: System.monotonic_time(:millisecond)
end
