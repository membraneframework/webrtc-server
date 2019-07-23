defmodule Membrane.WebRTC.Server.WebSocket do
  @behaviour :cowboy_websocket
  require Logger
  require Jason

  defmodule State do
    @enforce_keys [:module, :room, :peer_id]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            room: String.t(),
            peer_id: String.t(),
            module: module()
          }
  end

  @callback authenticate(:cowboy_req.req(), State.t()) :: {:ok, room: String.t()} | {:error, any}

  @callback on_init(:cowboy_req.req(), State.t()) ::
              {:cowboy_websocket, :cowboy_req.req()}
              | {:cowboy_websocket, :cowboy_req.req(), :cowboy_websocket.opts()}

  @callback on_websocket_init(State.t()) ::
              :ok | {:reply, :cow_ws.frame() | [:cow_ws.frame()]} | :stop

  def init(request, %{module: module} = args) do
    case(apply(module, :authenticate, [request, args])) do
      {:ok, room: room} ->
        state = %State{room: room, peer_id: make_peer_id(), module: module}
        {apply(module, :on_init, [request, state]), state}

      {:error, _} ->
        Logger.error("Authentication error")
        request = :cowboy_req.reply(403, request)
        {:ok, request, %{}}
    end
  end

  def websocket_init(%State{room: room, peer_id: peer_id} = state) do
    join_room(room, peer_id)
    {apply(state.module, :on_websocket_init, [state]), state}
  end

  def websocket_handle({:text, "ping"}, state) do
    {:reply, {:text, "pong"}, state}
  end

  def websocket_handle({:text, text}, state),
    do: text |> Jason.decode() |> handle_message(state)

  def websocket_handle(_, state) do
    Logger.warn("Non-text frame")
    {:ok, state}
  end

  def websocket_info(message, state) do
    {:reply, message, state}
  end

  def terminate(_, _, %State{room: room, peer_id: peer_id}) do
    Logger.info("Terminating peer #{peer_id}")
    leave_room(room, peer_id)
  end

  def terminate(_, _, _) do
    Logger.info("Terminating peer")
    :ok
  end

  defp handle_message(
         {:ok, %{"to" => peer_id, "data" => _} = message},
         %State{peer_id: my_peer_id, room: room} = state
       ) do
    Logger.info("Sending message to peer #{peer_id} from #{my_peer_id} in room #{room}")
    send_message(my_peer_id, peer_id, message, room)
    {:ok, state}
  end

  defp handle_message({:error, _}, state) do
    Logger.error("Wrong message")
    {:ok, encoded} = Jason.encode(%{"event" => :error, "description" => "invalid json"})
    {:reply, {:text, encoded}, state}
  end

  defp make_peer_id() do
    "#Reference" <> peer_id = Kernel.inspect(Kernel.make_ref())
    peer_id
  end

  defp join_room(room, peer_id) do
    if(Registry.match(Server.Registry, :room, room) == []) do
      {:ok, _} = create_room(room)
    end

    [{room_pid, ^room}] = Registry.match(Server.Registry, :room, room)

    {:ok, message} = Jason.encode(%{"event" => :joined, "data" => %{peer_id: peer_id}})

    GenServer.cast(room_pid, {:broadcast, {:text, message}})
    GenServer.cast(room_pid, {:add, peer_id, self()})
    {}
  end

  defp leave_room(room, peer_id) do
    case Registry.match(Server.Registry, :room, room) do
      [{room_pid, ^room}] ->
        {:ok, message} =
          Jason.encode(%{
            "event" => :left,
            "data" => %{"peer_id" => peer_id}
          })

        GenServer.cast(room_pid, {:remove, peer_id})
        GenServer.cast(room_pid, {:broadcast, {:text, message}})
        :ok

      [] ->
        Logger.error("Couldn't find room #{room}")
        {:error, %{}}
    end
  end

  defp create_room(room) do
    child_spec = {Membrane.WebRTC.Server.Room, %{name: room}}
    Logger.info("Creating room #{room}")
    DynamicSupervisor.start_child(Membrane.WebRTC.Server, child_spec)
  end

  defp send_message(from, to, message, room) do
    {:ok, message} = Map.put(message, "from", from) |> Jason.encode()
    [{room_pid, ^room}] = Registry.match(Server.Registry, :room, room)

    case GenServer.call(room_pid, {:send, {:text, message}, to}) do
      :ok ->
        :ok

      {:error, "no such peer"} ->
        Logger.error("Could not find peer")
        {:error, "no such peer"}

      _ ->
        Logger.error("Unknown error")
        {:error, :unknown}
    end
  end

  defmacro __using__(_) do
    quote location: :keep do
      @behaviour unquote(__MODULE__)
    end
  end
end