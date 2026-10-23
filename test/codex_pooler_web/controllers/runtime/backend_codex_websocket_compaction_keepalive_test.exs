defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketCompactionKeepaliveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.DownstreamKeepaliveScenario, only: [put_settings!: 2]

  alias CodexPooler.Accounting.{LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPoolerWeb.CodexResponsesSocket

  @moduletag capture_log: true
  @budget 15_000
  @fixture Path.expand("../../../fixtures/codex/rust-v0.153.3-b1a547b1f73ce86205d9222ac19cff334b3b7a2e/remote_compaction_v2_request.json", __DIR__)
  @external_resource @fixture
  @item %{"type" => "compaction", "encrypted_content" => "synthetic-final-ciphertext"}

  for forwarded? <- [false, true] do
    @tag forwarded: forwarded?
    test "forwarded=#{forwarded?}: withheld compaction stays live without leaking provisional output", %{forwarded: forwarded?} do
      put_settings!([websocket_idle_timeout_ms: 1_000], forwarded?)
      hold = make_ref()
      opening = %{"type" => "response.output_item.added", "item" => %{@item | "encrypted_content" => "synthetic-provisional-ciphertext"}}
      frames = [opening, %{"type" => "response.output_item.done", "item" => @item}, %{"type" => "response.completed", "response" => %{"id" => "resp_keepalive_compact", "status" => "completed", "output" => [@item]}}]
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.expect_request(method: "WEBSOCKET", respond: FakeUpstream.barrier_websocket_frames(Enum.map(frames, &CodexPooler.JSON.encode!/1), notify: self(), release_ref: hold))]))
      setup = gateway_setup(upstream, compact?: true)
      port = start_public_endpoint!()
      {conn, websocket, ref} = public_websocket_connect!(port, setup, Ecto.UUID.generate())
      payload = @fixture |> File.read!() |> CodexPooler.JSON.decode!() |> Map.fetch!("request") |> Map.merge(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => [%{"type" => "message", "role" => "user", "content" => "synthetic history"}, %{"type" => "compaction_trigger"}]}) |> CodexPooler.JSON.encode!()
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, payload)

      try do
        assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^hold}, @budget
        :ok = FakeUpstream.release_frame(upstream, hold)
        assert_receive {:fake_upstream_frame_barrier, 1, _handler, ^hold}, @budget
        started = System.monotonic_time(:millisecond)
        # Four actual keepalive/pong round trips span more than the native
        # listener's one-second idle bound, while the provider cannot finish.
        {conn, websocket, events, pings} = read_until(conn, websocket, ref, [], 0, fn events, pings -> length(events) >= 4 and pings >= 4 end)
        assert System.monotonic_time(:millisecond) - started >= 1_000
        assert Enum.all?(events, &(&1 == %{"type" => "keepalive"}))
        assert pings >= 4
        assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
        assert request.status == "in_progress"
        assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 0
        :ok = FakeUpstream.release_remaining_frames(upstream, hold)
        {conn, _websocket, events, _pings} = read_until(conn, websocket, ref, events, pings, fn events, _ -> Enum.any?(events, &(&1["type"] == "response.completed")) end)
        final = Enum.reject(events, &(&1["type"] == "keepalive"))
        assert Enum.map(final, & &1["type"]) == ["response.output_item.done", "response.completed"]
        assert hd(final)["item"] == @item
        refute inspect(events) =~ "synthetic-provisional-ciphertext"
        assert Repo.reload!(request).status == "succeeded"
        assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
        assert FakeUpstream.count(upstream) == 1
        Mint.HTTP.close(conn)
      after
        FakeUpstream.release_remaining_frames(upstream, hold)
        Mint.HTTP.close(conn)
      end
    end
  end

  test "keepalive ignores stale collectors and terminal delivery without inventing progress" do
    tick = {CodexResponsesSocket, :downstream_keepalive, 10_000, %{}}
    pid = self()
    base = %{tasks: MapSet.new(), response_task_compaction_collectors: MapSet.new([pid])}
    assert {:ok, ^base} = CodexResponsesSocket.handle_info(tick, base)
    terminal = %{base | tasks: MapSet.new([pid])} |> Map.put(:downstream_delivery_evidence, %{pid => %{frames: 0, terminal_class: "response.completed"}})
    assert {:ok, ^terminal} = CodexResponsesSocket.handle_info(tick, terminal)
    active = %{base | tasks: MapSet.new([pid])}
    assert {:push, [{:ping, ""}, {:text, ~s({"type":"keepalive"})}], ^active} = CodexResponsesSocket.handle_info(tick, active)
  end

  defp read_until(conn, websocket, ref, events, pings, done?) do
    if done?.(events, pings) do
      {conn, websocket, events, pings}
    else
      message = receive_mint_socket_message!(conn, @budget, "waiting for compaction keepalive")
      {:ok, conn, responses} = Mint.WebSocket.stream(conn, message)

      {conn, websocket, events, pings} = Enum.reduce(responses, {conn, websocket, events, pings}, &read_response(&1, &2, ref))

      read_until(conn, websocket, ref, events, pings, done?)
    end
  end

  defp read_response({:data, ref, data}, {conn, websocket, events, pings}, ref) do
    {:ok, websocket, frames} = Mint.WebSocket.decode(websocket, data)
    Enum.reduce(frames, {conn, websocket, events, pings}, &read_frame(&1, &2, ref))
  end

  defp read_response(_response, acc, _ref), do: acc

  defp read_frame({:ping, payload}, {conn, websocket, events, pings}, ref) do
    {:ok, websocket, encoded} = Mint.WebSocket.encode(websocket, {:pong, payload})
    {:ok, conn} = Mint.WebSocket.stream_request_body(conn, ref, encoded)
    {conn, websocket, events, pings + 1}
  end

  defp read_frame({:text, text}, {conn, websocket, events, pings}, _ref),
    do: {conn, websocket, events ++ [CodexPooler.JSON.decode!(text)], pings}
end
