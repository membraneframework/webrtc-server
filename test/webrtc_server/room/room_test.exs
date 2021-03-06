defmodule Membrane.WebRTC.Server.RoomTest do
  use ExUnit.Case, async: true

  alias Membrane.WebRTC.Server
  alias Server.{Message, RoomSupervisor}
  alias Server.Room.{Options, State}
  alias Server.Support.{MockRoom, MockSupervisor, RoomHelper}

  @module Membrane.WebRTC.Server.Room

  setup_all do
    child_spec = Registry.child_spec(keys: :unique, name: MockRegistry)
    start_supervised(child_spec)

    :ok
  end

  describe "handle_cast should" do
    test "not receive broadcasted message when broadcaster is given" do
      ping_message = %Message{event: "ping", to: "all"}
      @module.handle_call({:forward, ping_message}, nil, state(10, BiMap.new(), true))
      refute_received ^ping_message
    end

    test "not change state nor send messages when broadcasting to empty room" do
      ping_message = %Message{event: "ping", to: "all"}

      assert @module.handle_call({:forward, ping_message}, nil, state(0)) ==
               {:reply, :ok, state(0)}

      refute_received ^ping_message
    end
  end

  describe "handle_call should" do
    test "receive sent ping" do
      ping_message = %Message{event: "ping", to: ["peer_1"]}
      @module.handle_call({:forward, ping_message}, self(), state(5, BiMap.new(), true))
      assert_received ping_message
    end

    test "not return :ok nor receive ping if peer not exists" do
      new_state = state(5, BiMap.new(), true)
      ping_message = %Message{event: "ping", to: ["peer_-1"]}

      refute @module.handle_call({:forward, ping_message}, self(), new_state) ==
               {:reply, :ok, new_state}

      refute_received ^ping_message
    end

    test "add peer to room" do
      auth_data = RoomHelper.create_auth(2)
      pid = RoomHelper.generate_pid(2, false)

      assert @module.handle_call({:join, auth_data, pid}, self(), state(1)) ==
               {:reply, :ok, state(2)}
    end

    test "add peer to room with many peers" do
      auth_data = RoomHelper.create_auth(150)
      pid = RoomHelper.generate_pid(150, false)

      assert @module.handle_call(
               {:join, auth_data, pid},
               self(),
               state(149)
             ) ==
               {:reply, :ok, state(150)}
    end

    test "add peer to empty room" do
      auth_data = RoomHelper.create_auth(1)

      assert @module.handle_call({:join, auth_data, self()}, self(), state(0)) ==
               {:reply, :ok, state(1)}
    end

    test "replace already existing peer" do
      pid = RoomHelper.generate_pid(5, true)
      state = %State{peers: BiMap.new(%{"peer_1" => pid}), module: MockRoom}
      auth_data = RoomHelper.create_auth(1)

      assert @module.handle_call({:join, auth_data, pid}, self(), state(1)) ==
               {:reply, :ok, state}
    end
  end

  describe "create should" do
    test "start room under RoomSupervisor" do
      start_supervised(MockSupervisor)

      assert {:ok, pid} =
               @module.start_supervised(%Options{
                 name: "create_test",
                 module: MockRoom,
                 registry: MockRegistry
               })

      assert DynamicSupervisor.which_children(RoomSupervisor) == [
               {:undefined, pid, :worker, [Membrane.WebRTC.Server.Room]}
             ]

      @module.stop(pid)
    end
  end

  describe "init should" do
    test "registry itself" do
      assert {:ok, pid} =
               @module.start_link(%Options{name: "name", module: MockRoom, registry: MockRegistry})

      assert Registry.lookup(MockRegistry, "name") == [{pid, nil}]
      @module.stop(pid)
    end
  end

  describe "stop should" do
    test "terminate process" do
      assert {:ok, pid} =
               @module.start_link(%Options{name: "name", module: MockRoom, registry: MockRegistry})

      @module.stop(pid)
      Process.sleep(5)
      refute Process.alive?(pid)
    end
  end

  describe "terminate should" do
    test "unregistry itself and not cause Registry termination" do
      Application.start(:logger)
      auth_data = RoomHelper.create_auth("id")

      assert {:ok, room_pid} =
               @module.start_link(%Options{name: "room", module: MockRoom, registry: MockRegistry})

      assert {:ok, mock_pid} =
               @module.start_link(%Options{name: "mock", module: MockRoom, registry: MockRegistry})

      RoomHelper.join(room_pid, auth_data, RoomHelper.generate_pid(0, false))
      assert :ok == GenServer.stop(room_pid, :normal)
      Process.sleep(20)
      assert Registry.lookup(MockRegistry, "mock") == [{mock_pid, nil}]
      assert Registry.lookup(MockRegistry, "room") == []
    end
  end

  def state(number_of_peers, map \\ BiMap.new(), real \\ false) do
    case number_of_peers do
      0 ->
        %State{peers: map, module: MockRoom}

      1 ->
        state(0, BiMap.put(map, "peer_1", self()))

      n ->
        name = "peer_" <> to_string(n)
        pid = RoomHelper.generate_pid(n, real)
        state(n - 1, BiMap.put(map, name, pid))
    end
  end
end
