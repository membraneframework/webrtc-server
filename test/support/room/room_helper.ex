defmodule Membrane.WebRTC.Server.Support.RoomHelper do
  @moduledoc false

  alias Membrane.WebRTC.Server.Peer.AuthData

  def generate_pid(_number, true),
    do: spawn(fn -> :ok end)

  def generate_pid(number, false),
    do: IEx.Helpers.pid(0, number, 0)

  def create_auth(sufix),
    do: %AuthData{peer_id: "peer_" <> to_string(sufix), credentials: %{}, metadata: nil}
end