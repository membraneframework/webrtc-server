defmodule Example.Application do
  @moduledoc false
  use Application
  alias Membrane.WebRTC.Server.Peer.Options
  alias Membrane.WebRTC.Server.Room

  @impl true
  def start(_type, _args) do
    Room.start_supervised(%Room.Options{name: "room", module: Example.Room})

    children = [
      Plug.Cowboy.child_spec(
        scheme: Application.fetch_env!(:example, :scheme),
        plug: Example.Router,
        options: [
          dispatch: dispatch(),
          port: Application.fetch_env!(:example, :port),
          ip: Application.fetch_env!(:example, :ip),
          password: Application.fetch_env!(:example, :password),
          otp_app: :example,
          keyfile: Application.fetch_env!(:example, :keyfile),
          certfile: Application.fetch_env!(:example, :certfile)
        ]
      )
    ]

    opts = [strategy: :one_for_one, name: Example.Application]
    Supervisor.start_link(children, opts)
  end

  defp dispatch do
    options = %Options{module: Example.Peer}

    [
      {:_,
       [
         {"/server/[:room]/[:username]/[:password]/", Membrane.WebRTC.Server.Peer, options},
         {:_, Plug.Cowboy.Handler, {Example.Router, []}}
       ]}
    ]
  end
end
