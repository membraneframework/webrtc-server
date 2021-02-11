defmodule Membrane.WebRTC.Server.Room do
  @moduledoc """
  A behaviuor module for WebRTC room that manages peers and mediate in their communication.

  Rooms have to be created explicitly (preferably by `start_supervised/2` function).
  """

  use GenServer
  require Logger
  alias Membrane.WebRTC.Server.{Message, Peer, RoomSupervisor}
  alias Membrane.WebRTC.Server.Peer.AuthData
  alias Membrane.WebRTC.Server.Room.{Options, State}

  @typedoc """
  Defines a custom state of the room, passed as argument and returned by callbacks.
  """
  @type internal_state :: any()

  @doc """
  Callback invoked when a room is created.
  """
  @callback on_init(options :: Options.custom_options()) :: {:ok, internal_state()}

  @doc """
  Callback invoked when a peer is about to join the room.

  Useful for authorizing or performing other checks (e.g. controlling number of peers in room).

  Returning `{:error, error}` will cause peer sending
  `t:Membrane.WebRTC.Server.Message.error_message/0` to the client and closing WebSocket.
  """
  @callback on_join(auth_data :: AuthData.t(), state :: internal_state()) ::
              {:ok, internal_state()} | {{:error, error :: atom()}, internal_state()}

  @doc """
  Callback invoked when a peer is leaving the room.
  """
  @callback on_leave(peer_id :: Peer.peer_id(), state :: internal_state()) ::
              {:ok, internal_state()}

  @doc """
  Callback invoked before forwarding messages either peers.

  This mean this callback will be invoked every time message is forwarded or the room broadcasts
  messages by itself (e.g. when peer joins the room).

  Room will forward_message message returned by this callback, ergo returning `{:ok, state}`
  will cause ignoring message.
  """
  @callback on_forward(
              message :: Message.t(),
              state :: internal_state()
            ) :: {:ok, internal_state()} | {:ok, Message.t(), internal_state()}

  @doc """
  Callback invoked when the room is shutting down.

  Useful for any cleanup required.
  """
  @callback on_terminate(state :: internal_state()) :: :ok

  @optional_callbacks on_init: 1,
                      on_join: 2,
                      on_leave: 2,
                      on_forward: 2,
                      on_terminate: 1

  @doc """
  Starts a room based on the `options.module`, registers it in `options.registry`
  (under the `options.name`) and links it to the current process.

  `options.custom_options` are passed to module's `c:on_init/1` callback.
  """
  @spec start_link(options :: Options.t()) :: GenServer.on_start()
  def start_link(options) do
    name = {:via, Registry, {options.registry, options.name}}
    GenServer.start_link(__MODULE__, options, name: name)
  end

  @doc """
  Creates a room under the default room supervisor.

  Calls `start_link/1` underneath.
  """
  @spec start_supervised(options :: Options.t()) ::
          DynamicSupervisor.on_start_child()
  def start_supervised(options) do
    child_spec = {Membrane.WebRTC.Server.Room, options}
    DynamicSupervisor.start_child(RoomSupervisor, child_spec)
  end

  @doc """
  Forwards the message to the addressees given under `message.to` key.

  Messages ment to be broadcasted should have `message.to` set to "all". Broadcasted message will
  be forwarded to all peers, except for sender (given under `message.from`).
  """
  @spec forward_message(room :: pid(), message :: Message.t()) ::
          :ok | {:error, :no_such_peer} | {:error, :unknown_error}
  def forward_message(room, %Message{to: peer_id} = message) when peer_id != nil do
    case GenServer.call(room, {:forward, message}) do
      :ok ->
        :ok

      {:error, :no_such_peer} ->
        Logger.error("Could not find peer")
        {:error, :no_such_peer}

      {:error, error} ->
        Logger.error("Unknown error, details: #{inspect(error)}")
        {:error, error}
    end
  end

  @doc """
  Stops room process.
  """
  @spec stop(room :: pid()) :: :ok
  def stop(room) do
    GenServer.cast(room, :stop)
  end

  @impl true
  def init(options) do
    state = %State{peers: BiMap.new(), module: options.module}
    callback_exec(:on_init, [options.custom_options], state)
  end

  @impl true
  def handle_call({:forward, message}, _from, state) do
    case try_forward_message(message, state) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_call({:join, auth_data, peer_pid}, _from, state) do
    case callback_exec(:on_join, [auth_data], state) do
      {:ok, state} ->
        peers = state.peers |> BiMap.put(auth_data.peer_id, peer_pid)
        state = %State{state | peers: peers}

        Process.monitor(peer_pid)

        broadcasted_message = %Message{
          data: %{peer_id: auth_data.peer_id},
          event: "joined",
          from: auth_data.peer_id,
          from_metadata: auth_data.metadata,
          to: "all"
        }

        {:ok, state} = try_forward_message(broadcasted_message, state)
        {:reply, :ok, state}

      {{:error, error}, state} ->
        {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_cast(:stop, state),
    do: {:stop, :normal, state}

  @impl true
  def handle_info({:DOWN, _reference, :process, pid, _reason}, state) do
    peer_id = BiMap.get_key(state.peers, pid)
    {:noreply, leave(peer_id, state)}
  end

  @impl true
  def terminate(_reason, state) do
    callback_exec(:on_terminate, [], state)
  end

  defp leave(peer_id, state) do
    if BiMap.has_key?(state.peers, peer_id) do
      {:ok, internal_state} = callback_exec(:on_leave, [peer_id], state)
      peers = BiMap.delete_key(state.peers, peer_id)

      new_state =
        state
        |> Map.put(:peers, peers)
        |> Map.put(:internal_state, internal_state)

      message = %Message{event: "left", data: %{peer_id: peer_id}, to: "all"}
      try_forward_message(message, new_state)
      new_state
    else
      state
    end
  end

  defp callback_exec(:on_init, args, state) do
    {:ok, internal_state} = apply(state.module, :on_init, args)
    {:ok, %State{state | internal_state: internal_state}}
  end

  defp callback_exec(:on_join, args, state) do
    case apply(state.module, :on_join, args ++ [state.internal_state]) do
      {:ok, internal_state} ->
        state = %State{state | internal_state: internal_state}
        {:ok, state}

      {{:error, error}, internal_state} ->
        state = %State{state | internal_state: internal_state}
        {{:error, error}, state}
    end
  end

  defp callback_exec(:on_leave, args, state),
    do: apply(state.module, :on_leave, args ++ [state.internal_state])

  defp callback_exec(:on_forward, args, state) do
    case apply(state.module, :on_forward, args ++ [state.internal_state]) do
      {:ok, internal_state} ->
        {:ok, %State{state | internal_state: internal_state}}

      {:ok, message, internal_state} ->
        {:ok, message, %State{state | internal_state: internal_state}}
    end
  end

  defp callback_exec(:on_terminate, args, state),
    do: apply(state.module, :on_terminate, args ++ [state.internal_state])

  defp try_forward_message(message, state) do
    case callback_exec(:on_forward, [message], state) do
      {:ok, state} ->
        {:ok, state}

      {:ok, %Message{to: "all"} = message, state} ->
        forward_to_all(message, state.peers)
        {:ok, state}

      {:ok, message, state} ->
        addressees = message.to

        with :ok <-
               Bunch.Enum.try_each(addressees, fn peer -> check_peer(peer, state.peers) end) do
          Enum.each(addressees, fn peer ->
            BiMap.get(state.peers, peer) |> Peer.send_to_client(message)
          end)

          {:ok, state}
        end
    end
  end

  defp forward_to_all(%Message{from: nil} = message, peers) do
    Enum.each(peers, fn {_peer_id, pid} ->
      Peer.send_to_client(pid, message)
    end)

    :ok
  end

  defp forward_to_all(message, peers) do
    peers
    |> BiMap.delete_key(message.from)
    |> Enum.each(fn {_peer_id, pid} ->
      Peer.send_to_client(pid, message)
    end)

    :ok
  end

  defp check_peer(adressee, peers) do
    case BiMap.get(peers, adressee) do
      nil ->
        {:error, :no_such_peer}

      _pid ->
        :ok
    end
  end

  defmacro __using__(_) do
    quote location: :keep do
      @behaviour unquote(__MODULE__)

      @impl true
      def on_init(_args),
        do: {:ok, %{}}

      @impl true
      def on_join(_auth_data, state),
        do: {:ok, state}

      @impl true
      def on_leave(_peer_id, state),
        do: {:ok, state}

      @impl true
      def on_forward(message, state) do
        {:ok, message, state}
      end

      @impl true
      def on_terminate(_state),
        do: :ok

      defoverridable on_init: 1,
                     on_join: 2,
                     on_leave: 2,
                     on_forward: 2,
                     on_terminate: 1
    end
  end
end
