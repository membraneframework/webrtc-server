defmodule Membrane.WebRTC.Server.Peer do
  @moduledoc """
  Module that manages websocket lifecycle and communication with client.

  Every message received from client must be JSON matching 
  `Membrane.WebRTC.Server.Message` struct. 

  Every Erlang message received in form of `%Membrane.WebRTC.Server.Message{}`
  (i.e. messages about peers joining/leaving room, ICE candidates from other peers)
  will be encoded into JSON and passed to client.

  Implementation of 
  [`Cowboy WebSocket`](https://ninenines.eu/docs/en/cowboy/2.6/manual/cowboy_websocket/).
  """

  @behaviour :cowboy_websocket
  require Logger
  alias __MODULE__.{Context, Options, State}
  alias Membrane.WebRTC.Server.{Message, Room}

  @typedoc """
  Defines custom state of Peer, passed as argument and returned by callbacks. 
  """
  @type internal_state :: any

  @typedoc """
  Defines possible termination reasons passed to `c:terminate/2` callback.
  """
  @type terminate_reason ::
          :normal
          | :stop
          | :timeout
          | :remote
          | {:remote, :cow_ws.close_code(), binary()}
          | {:error, :badencoding | :badframe | :closed | atom()}
          | {:crash, :error | :exit | :throw, any()}

  @doc """
  Callback invoked before initialization of WebSocket.
  Peer will later join (or create and join) room with name returned by callback.
  Returning `{:error, reason}` will cause aborting initialization of WebSocket
  and returning reply with 401 status code and the same request as given.

  This callback is optional.
  """
  @callback authenticate(request :: :cowboy_req.req(), options :: any) ::
              {:ok, %{room: String.t(), state: internal_state}}
              | {:ok, %{room: String.t()}}
              | {:error, reason :: any}

  @doc """
  Callback invoked before initialization of WebSocket, after successful authentication.
  Useful for setting custom Cowboy WebSocket options, like timeout or maximal frame size.

  This callback is optional.
  """
  @callback on_init(
              request :: :cowboy_req.req(),
              context :: Context.t(),
              state :: internal_state
            ) ::
              {:cowboy_websocket, :cowboy_req.req(), internal_state}
              | {:cowboy_websocket, :cowboy_req.req(), internal_state, :cowboy_websocket.opts()}

  @doc """
  Callback invoked after initialization of WebSocket.
  Useful for setting up internal state or informing client about successful authentication
  and initialization. 

  This callback is optional.
  """
  @callback on_websocket_init(context :: Context.t(), state :: internal_state) ::
              {:ok, internal_state}
              | {:ok, internal_state, :hibernate}
              | {:reply, :cow_ws.frame() | [:cow_ws.frame()], internal_state}
              | {:reply, :cow_ws.frame() | [:cow_ws.frame()], internal_state, :hibernate}
              | {:stop, internal_state}

  @doc """
  Callback invoked after successful decoding received JSON message.
  Peer will proceed to send message returned by this callback to Room,
  ergo returning `{:ok, state}` will cause ignoring message.
  Useful for modyfing or ignoring messages.

  This callback is optional.
  """
  @callback on_message(message :: Message.t(), context :: Context.t(), state :: internal_state) ::
              {:ok, Message.t(), internal_state}
              | {:ok, internal_state}

  @doc """
  Callback invoked when peer is shutting down.
  Internally called in `:cowboy_websocket.terminate/3` callback.
  Useful for any cleanup required.

  This callback is optional.
  """
  @callback on_terminate(
              reason :: terminate_reason,
              partial_req :: :cowboy_req.req(),
              context :: Context.t(),
              state :: internal_state
            ) :: :ok

  defmodule DefaultRoom do
    @moduledoc false
    use Room
  end

  @impl true
  def init(request, %Options{module: module, room_module: room_module} = options) do
    case callback_exec(:authenticate, [request], options) do
      {:ok, %{room: room, state: internal_state}} ->
        state = %State{
          room: room,
          peer_id: UUID.uuid1(),
          module: module,
          internal_state: internal_state,
          room_module: room_module
        }

        callback_exec(:on_init, [request], state)

      {:error, reason} ->
        Logger.error("Authentication error, reason: #{inspect(reason)}")
        request = :cowboy_req.reply(401, request)
        {:ok, request, %{}}
    end
  end

  @impl true
  def websocket_init(%State{room: room, peer_id: peer_id} = state) do
    room_pid = get_room_pid!(room, state)
    Room.join(room_pid, peer_id, self())
    Process.monitor(room_pid)
    callback_exec(:on_websocket_init, [], state)
  end

  @impl true
  def websocket_handle({:text, "ping"}, state) do
    {:reply, {:text, "pong"}, state}
  end

  @impl true
  def websocket_handle(:ping, state),
    do: {:reply, :pong, state}

  @impl true
  def websocket_handle({:ping, data}, state),
    do: {:reply, {:pong, data}, state}

  @impl true
  def websocket_handle({:text, message}, state),
    do: message |> Jason.decode() |> handle_message(state)

  @impl true
  def websocket_handle(_frame, state) do
    Logger.warn("Non-text frame")
    {:ok, state}
  end

  @impl true
  def websocket_info(%Message{} = message, state) do
    encoded = message |> Jason.encode!()
    {:reply, {:text, encoded}, state}
  end

  @impl true
  def websocket_info({:DOWN, _reference, :process, _pid, reason}, state) do
    message = %Message{event: "room_closed", data: %{reason: reason}}
    send(self(), message)
    {:stop, state}
  end

  @impl true
  def terminate(reason, req, %State{peer_id: peer_id} = state) do
    Logger.info("Terminating peer #{peer_id}")
    callback_exec(:on_terminate, [reason, req], state)
  end

  defp callback_exec(:authenticate, args, options) do
    with {:ok, room: room} <- apply_callback(:authenticate, args, options) do
      {:ok, %{room: room, state: nil}}
    end
  end

  defp callback_exec(:on_init, [request], state) do
    case apply_callback(:on_init, [request], state) do
      {:cowboy_websocket, request, internal_state} ->
        {:cowboy_websocket, request, %State{state | internal_state: internal_state}}

      {:cowboy_websocket, request, internal_state, opts} ->
        {:cowboy_websocket, request, %State{state | internal_state: internal_state}, opts}
    end
  end

  defp callback_exec(:on_websocket_init, [], state) do
    case apply_callback(:on_websocket_init, [], state) do
      {:ok, internal_state} ->
        {:ok, %State{state | internal_state: internal_state}}

      {:ok, internal_state, :hibernate} ->
        {:ok, %State{state | internal_state: internal_state}, :hibernate}

      {:reply, frames, internal_state} ->
        {:reply, frames, %State{state | internal_state: internal_state}}

      {:reply, frames, internal_state, :hibernate} ->
        {:reply, frames, %State{state | internal_state: internal_state}, :hibernate}

      {:stop, internal_state} ->
        {:stop, %State{state | internal_state: internal_state}}
    end
  end

  defp callback_exec(:on_message, [message], state) do
    case apply_callback(:on_message, [message], state) do
      {:ok, internal_state} ->
        {:ok, %State{state | internal_state: internal_state}}

      {:ok, %Message{} = message, internal_state} ->
        room_pid = get_room_pid!(state.room, state)
        Room.send_message(room_pid, message)
        {:ok, %State{state | internal_state: internal_state}}
    end
  end

  defp callback_exec(:on_terminate, args, state) do
    apply_callback(:on_terminate, args, state)
  end

  defp apply_callback(:authenticate, args, options) do
    args = args ++ [options.custom_options]
    apply(options.module, :authenticate, args)
  end

  defp apply_callback(callback, args, state) do
    args = args ++ [%Context{peer_id: state.peer_id, room: state.room}, state.internal_state]
    apply(state.module, callback, args)
  end

  defp handle_message(
         {:ok, %{"event" => _event} = message},
         %State{peer_id: peer_id} = state
       ) do
    message =
      message
      |> Bunch.Map.map_keys(fn string -> String.to_atom(string) end)
      |> Map.put(:from, peer_id)

    message = struct(Message, message)
    callback_exec(:on_message, [message], state)
  end

  defp handle_message({:ok, _message}, state) do
    send(self(), %Message{event: "error", data: %{desciption: "Invalid message"}})
    {:ok, state}
  end

  defp handle_message({:error, jason_error}, state) do
    Logger.warn("Wrong message")

    send(self(), %Message{
      event: "error",
      data: %{description: "Invalid JSON", details: jason_error}
    })

    {:ok, state}
  end

  defp get_room_pid!(room, state) do
    case get_room_pid(room, state) do
      {:ok, pid} when is_pid(pid) -> pid
      {:ok, pid, _info} when is_pid(pid) -> pid
      {:error, tuple} when is_tuple(tuple) -> raise inspect(tuple)
      {:error, error} -> raise error
      {:error, error, message} -> raise error, message
    end
  end

  defp get_room_pid(room, %State{room_module: room_module}) do
    case Registry.lookup(Server.Registry, room) do
      [{room_pid, :room}] when is_pid(room_pid) ->
        {:ok, room_pid}

      [] ->
        Room.create(room, room_module)

      match ->
        {:error, MatchError, term: match}
    end
  end

  defmacro __using__(_) do
    quote location: :keep do
      @behaviour unquote(__MODULE__)

      def authenticate(_request, _options),
        do: {:ok, room: "room"}

      def on_init(request, _context, state) do
        opts = %{idle_timeout: 1000 * 60 * 15}
        {:cowboy_websocket, request, state, opts}
      end

      def on_websocket_init(_context, state),
        do: {:ok, state}

      def on_message(message, _context, state),
        do: {:ok, message, state}

      def on_terminate(_reason, _req, _context, _state),
        do: :ok

      defoverridable authenticate: 2,
                     on_init: 3,
                     on_websocket_init: 2,
                     on_message: 3,
                     on_terminate: 4
    end
  end
end
