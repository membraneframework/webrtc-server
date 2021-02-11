defmodule Membrane.WebRTC.Server.Peer do
  @moduledoc """
  Module that manages websocket lifecycle and communication with client.

  Adapts the
  [`:cowboy_websocket`](https://ninenines.eu/docs/en/cowboy/2.6/manual/cowboy_websocket/)
  behaviour.
  """

  @behaviour :cowboy_websocket
  require Logger
  alias __MODULE__.{AuthData, Context, Options, State}
  alias Membrane.WebRTC.Server.{Message, Room}

  @timeout 15 * 60 * 1000

  @typedoc """
  Defines unique indetifier (UUIDv4) bound to peer.
  """
  @type peer_id :: String.t()

  @typedoc """
  Defines custom state of a peer, passed as argument and returned by callbacks.
  """
  @type internal_state :: any()

  @doc """
  Callback invoked to extract credentials and metadata from request.

  After successfully parsing the request, `{:ok, credentials, metadata, room_name}` should be
  returned. Credentials and metadata will be used to create `Membrane.WebRTC.Server.Peer.AuthData`
  which is passed to `c:on_init/3` and `c:Membrane.WebRTC.Server.Room.on_join/2`.

  Returning `{:error, details}` will abort initialization of WebSocket
  and return a response with status 400.

  Peer will later try to join the room registered under `room_name`.
  If no such room can be found, peer will abort initialization of WebSocket
  and return a response with status 404.
  """
  @callback parse_request(request :: :cowboy_req.req()) ::
              {:ok, credentials :: map(), metadata :: any(), room_name :: String.t()}
              | {:error, cause :: any()}

  @doc """
  Callback invoked when a peer process is started.

  Useful both for confirming the identity of the client, as well as setting up state and/or
  custom [Cowboy WebSocket](https://ninenines.eu/docs/en/cowboy/2.6/manual/cowboy_websocket/)
  options, like timeout or maximal frame size.

  Options argument is value given under the `:custom_options` field in
  `Membrane.WebRTC.Server.Options`.

  Returning `{:ok, websocket_options, state}` will set up WebSocket options to the ones returned.

  Returning `{:ok, state}` will set up default WebSocket options with #{div(@timeout, 60000)}
  minutes timeout.

  Returning `{:error, details}` will abort initialization of WebSocket
  and return a response with status 401.
  """
  @callback on_init(
              context :: Context.t(),
              auth_data :: AuthData.t(),
              options :: any()
            ) ::
              {:ok, state :: internal_state}
              | {:ok, websocket_options :: :cowboy_websocket.opts(), state :: internal_state}
              | {:error, cause :: any()}

  @doc """
  Callback invoked after successful decoding received JSON message.

  Peer will proceed to send message returned by this callback to room,
  ergo returning `{:ok, state}` will cause ignoring message.

  Useful for modifying or ignoring messages.
  """
  @callback on_receive(message :: Message.t(), context :: Context.t(), state :: internal_state) ::
              {:ok, message :: Message.t(), state :: internal_state}
              | {:ok, state :: internal_state}

  @doc """
  Callback invoked when a peer is shutting down.

  Useful for any cleanup required.
  """
  @callback on_terminate(
              context :: Context.t(),
              state :: internal_state
            ) :: :ok

  @optional_callbacks on_init: 3,
                      on_receive: 3,
                      on_terminate: 2

  @doc """
  Encodes message into JSON and sends it to client.
  """
  @spec send_to_client(peer :: pid(), message :: Message.t()) :: :ok
  def send_to_client(peer, message) do
    encoded = Jason.encode!(message)
    send(peer, {:message, encoded})
    :ok
  end

  @doc """
  Stops peer process.
  """
  @spec stop(peer :: pid()) :: :ok
  def stop(peer) do
    send(peer, :stop)
    :ok
  end

  @impl true
  def init(request, %Options{} = options) do
    peer_id = UUID.uuid4()

    with {:ok, auth_data, room_name} <-
           callback_exec(:parse_request, [request], options, peer_id),
         {:ok, room} <- get_room_pid(room_name, options.registry),
         {:ok, websocket_options, internal_state} <-
           callback_exec(
             :on_init,
             [%Context{peer_id: peer_id, room: room}, auth_data],
             options
           ) do
      state = %State{
        module: options.module,
        room: room,
        peer_id: peer_id,
        internal_state: internal_state,
        auth_data: auth_data,
        metadata: auth_data.metadata
      }

      {:cowboy_websocket, request, state, websocket_options}
    else
      {:error, {:could_not_parse, details}} ->
        Logger.error("Could not parse auth request, details: #{inspect(details)}")
        reply = :cowboy_req.reply(400, request)
        {:ok, reply, %{}}

      {:error, {:no_such_room, room}} ->
        Logger.error("Could not find room named #{room}")
        reply = :cowboy_req.reply(404, request)
        {:ok, reply, %{}}

      {:error, {:init_failed, details}} ->
        Logger.error("Authentication error, details: #{inspect(details)}")
        reply = :cowboy_req.reply(401, request)
        {:ok, reply, %{}}
    end
  end

  @impl true
  def websocket_init(%State{} = state) do
    case join_room(state) do
      :ok ->
        state = %State{state | auth_data: :already_authorised}

        message = %Message{
          event: "authenticated",
          data: %{peer_id: state.peer_id},
          to: [state.peer_id]
        }

        send_to_client(self(), message)
        {:ok, state}

      {:error, {error, details}} ->
        error_log = to_string(error) |> String.replace("_", " ") |> String.capitalize()
        Logger.error("#{error_log}, details: #{inspect(details)}")

        stop_and_send_error(self(), error_log, details, state)
        {:ok, state}
    end
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
  def websocket_info({:message, message}, state) do
    {:reply, {:text, message}, state}
  end

  @impl true
  def websocket_info(:stop, state) do
    {:stop, state}
  end

  @impl true
  def websocket_info({:DOWN, _ref, :process, room, reason}, %State{room: room} = state) do
    stop_and_send_error(self(), "Room closed", reason, state)
    {:ok, state}
  end

  @impl true
  def websocket_info(_message, state) do
    {:ok, state}
  end

  @impl true
  def terminate(_reason, _req, %State{} = state) do
    callback_exec(:on_terminate, [], state)
  end

  @impl true
  def terminate(_reason, _req, _options), do: :ok

  defp callback_exec(:parse_request, args, options, peer_id) do
    case apply_callback(:parse_request, args, options) do
      {:ok, credentials, metadata, room} ->
        auth_data = %AuthData{
          credentials: credentials,
          metadata: metadata,
          peer_id: peer_id
        }

        {:ok, auth_data, room}

      {:error, details} ->
        {:error, {:could_not_parse, details}}
    end
  end

  defp callback_exec(:on_init, args, options) do
    case apply_callback(:on_init, args, options) do
      {:ok, internal_state} ->
        websocket_options = %{idle_timeout: @timeout}
        {:ok, websocket_options, internal_state}

      {:ok, websocket_options, internal_state} ->
        {:ok, websocket_options, internal_state}

      {:error, details} ->
        {:error, {:init_failed, details}}
    end
  end

  defp callback_exec(:on_receive, [message], state) do
    case apply_callback(:on_receive, [message], state) do
      {:ok, internal_state} ->
        {:ok, %State{state | internal_state: internal_state}}

      {:ok, %Message{} = message, internal_state} ->
        Room.forward_message(state.room, message)
        {:ok, %State{state | internal_state: internal_state}}
    end
  end

  defp callback_exec(:on_terminate, args, state) do
    apply_callback(:on_terminate, args, state)
  end

  defp apply_callback(:on_init, args, options) do
    args = args ++ [options.custom_options]
    apply(options.module, :on_init, args)
  end

  defp apply_callback(:parse_request, args, options) do
    apply(options.module, :parse_request, args)
  end

  defp apply_callback(callback, args, state) do
    args = args ++ [%Context{peer_id: state.peer_id, room: state.room}, state.internal_state]
    apply(state.module, callback, args)
  end

  defp join_room(state) do
    case GenServer.call(state.room, {:join, state.auth_data, self()}) do
      :ok ->
        Process.monitor(state.room)
        :ok

      {:error, error} ->
        {:error, {:could_not_join_room, error}}
    end
  end

  defp stop_and_send_error(peer, error, details, state) do
    message = %Message{
      event: "error",
      data: %{description: error, details: details},
      to: [state.peer_id]
    }

    send_to_client(peer, message)
    stop(peer)
  end

  defp handle_message(
         {:ok, %{"event" => _event} = message},
         state
       ) do
    message = %Message{
      data: message["data"],
      event: message["event"],
      from: state.peer_id,
      from_metadata: state.metadata,
      to: message["to"]
    }

    callback_exec(:on_receive, [message], state)
  end

  defp handle_message({:ok, _message}, state) do
    send_to_client(self(), %Message{
      event: "error",
      data: %{description: "Invalid message"},
      to: [state.peer_id]
    })

    {:ok, state}
  end

  defp handle_message({:error, jason_error}, state) do
    Logger.warn("Wrong message")

    send_to_client(self(), %Message{
      event: "error",
      data: %{description: "Invalid JSON", details: jason_error.data}
    })

    {:ok, state}
  end

  defp get_room_pid(room, registry) do
    case Registry.lookup(registry, room) do
      [{room_pid, _value}] when is_pid(room_pid) ->
        {:ok, room_pid}

      [] ->
        {:error, {:no_such_room, room}}
    end
  end

  defmacro __using__(_) do
    quote location: :keep do
      @behaviour unquote(__MODULE__)

      @impl true
      def on_init(_context, _auth_data, options) do
        {:ok, options}
      end

      @impl true
      def on_receive(message, _context, state),
        do: {:ok, message, state}

      @impl true
      def on_terminate(_context, _state),
        do: :ok

      defoverridable on_init: 3,
                     on_receive: 3,
                     on_terminate: 2
    end
  end
end
