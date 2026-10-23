defmodule CodexPoolerWeb.Runtime.BackendCodexCompactionKeepaliveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 2, native_text_input: 1, start_public_endpoint!: 0, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.Repo

  @budget 15_000
  @keepalive ~s(event: keepalive\ndata: {"type":"keepalive"}\n\n)
  @item %{"type" => "compaction", "encrypted_content" => "synthetic-final-ciphertext"}

  setup do
    previous = Application.get_env(:codex_pooler, OperationalSettings)
    Application.put_env(:codex_pooler, OperationalSettings, settings: %OperationalSettings{sse_keepalive_interval_ms: 20})

    on_exit(fn ->
      if previous,
        do: Application.put_env(:codex_pooler, OperationalSettings, previous),
        else: Application.delete_env(:codex_pooler, OperationalSettings)
    end)
  end

  test "held compaction sends repeated parsed-event keepalives before validated output" do
    exercise_held_stream(
      success_tail(),
      fn body ->
        assert body =~ "synthetic-final-ciphertext"
        refute body =~ "synthetic-provisional-ciphertext"
        assert types(body) |> Enum.reject(&(&1 == "keepalive")) == ["response.output_item.done", "response.completed"]
      end,
      "succeeded",
      nil
    )
  end

  test "late quota failure retains its fatal code without leaking provider text" do
    tail = [{"response.failed", %{"type" => "response.failed", "response" => %{"status" => "failed", "error" => %{"code" => "insufficient_quota", "message" => "private-provider-text"}}}}]

    log =
      capture_log(fn ->
        exercise_held_stream(
          tail,
          fn body ->
            assert body =~ "insufficient_quota"
            refute body =~ "private-provider-text"
            refute body =~ "synthetic-provisional-ciphertext"
            refute body =~ "response.completed"
            assert List.last(types(body)) == "response.failed"
          end,
          "failed",
          "invalid_compaction_response"
        )
      end)

    assert log =~ "code=insufficient_quota"
    refute log =~ "private-provider-text"
  end

  test "late invalid duplicate output is rejected rather than streamed to the client" do
    tail = [done(@item), done(@item), completed()]

    log =
      capture_log(fn ->
        exercise_held_stream(
          tail,
          fn body ->
            assert body =~ "invalid_compaction_response"
            refute body =~ "synthetic-final-ciphertext"
            refute body =~ "synthetic-provisional-ciphertext"
            refute body =~ "response.completed"
          end,
          "failed",
          "invalid_compaction_response"
        )
      end)

    assert log =~ "compaction_invalid_reason=duplicate_compaction"
  end

  test "rollout drain interrupts a held compaction without waiting for provider completion" do
    log =
      capture_log(fn ->
        exercise_held_stream(
          success_tail(),
          fn body ->
            assert List.last(types(body)) == "response.failed"
            refute body =~ "ciphertext"
            refute body =~ "response.completed"
          end,
          "failed",
          "owner_drained",
          :drain
        )
      end)

    refute log =~ "private-provider-text"
  end

  defp exercise_held_stream(tail, assertion, status, error_code, action \\ :complete) do
    release = make_ref()
    opening = {"response.output_item.added", %{"type" => "response.output_item.added", "item" => %{"type" => "compaction", "encrypted_content" => "synthetic-provisional-ciphertext"}}}
    upstream = start_upstream(FakeUpstream.barrier_sse_stream([opening | tail], barrier_after: 1, notify: self(), release_ref: release, done: false, on_client_close: :expected))
    setup = gateway_setup(upstream, compact?: true)
    {conn, ref} = connect(setup)
    assert_receive {:fake_upstream_chunk_barrier, 1, provider, ^release}, @budget

    try do
      # The provider cannot finish until this exact barrier is released. Seeing
      # two actual socket events here fails on the original buffering path.
      {conn, held, false} = read_until(conn, ref, "", &(length(:binary.matches(&1, @keepalive)) >= 2))
      assert Enum.uniq(types(held)) == ["keepalive"]
      refute held =~ "ciphertext"
      [entry] = Enum.filter(DeferredStreamRegistry.streams(), &(&1.phase == :streaming))

      if action == :drain,
        do: DeferredStreamRegistry.interrupt(entry.token),
        else: send(provider, {:fake_upstream_release_chunk, release})

      {conn, body, true} = read_until(conn, ref, held, fn _body -> false end)
      assertion.(body)
      Mint.HTTP.close(conn)

      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert request.status == status
      assert request.response_status_code == 200
      assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
      assert attempt.network_error_code == error_code
      assert attempt.status == status
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
      assert FakeUpstream.http_request_count(upstream) == 1
    after
      Mint.HTTP.close(conn)
      send(provider, {:fake_upstream_release_chunk, release})
    end
  end

  defp connect(setup) do
    port = start_public_endpoint!()
    {:ok, conn} = Mint.HTTP.connect(:http, "localhost", port, mode: :passive)

    payload = %{
      "model" => setup.model.exposed_model_id,
      "input" => native_text_input("synthetic slow compaction") ++ [%{"type" => "compaction_trigger"}],
      "stream" => true,
      "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"compaction" => %{"implementation" => "responses_compaction_v2"}})}
    }

    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", "/backend-api/codex/responses", [{"authorization", setup.authorization}, {"content-type", "application/json"}], CodexPooler.JSON.encode!(payload))
    {conn, ref}
  end

  defp read_until(conn, ref, body, predicate), do: read_until(conn, ref, body, predicate, System.monotonic_time(:millisecond) + @budget)

  defp read_until(conn, ref, body, predicate, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)
    assert remaining > 0, "compaction stream did not reach the expected boundary"
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, remaining)

    {body, done?} =
      Enum.reduce(responses, {body, false}, fn
        {:status, ^ref, status}, acc ->
          assert status == 200
          acc

        {:data, ^ref, data}, {body, done?} ->
          {body <> data, done?}

        {:done, ^ref}, {body, _done?} ->
          {body, true}

        _headers, acc ->
          acc
      end)

    if done? or predicate.(body), do: {conn, body, done?}, else: read_until(conn, ref, body, predicate, deadline)
  end

  defp types(body) do
    body |> String.split("\n") |> Enum.filter(&(String.starts_with?(&1, "data: ") and &1 != "data: [DONE]")) |> Enum.map(fn line -> line |> String.trim_leading("data: ") |> CodexPooler.JSON.decode!() |> Map.fetch!("type") end)
  end

  defp success_tail, do: [done(@item), completed()]
  defp done(item), do: {"response.output_item.done", %{"type" => "response.output_item.done", "item" => item}}
  defp completed, do: {"response.completed", %{"type" => "response.completed", "response" => %{"id" => "synthetic-compact-response", "status" => "completed", "output" => [@item]}}}
end
