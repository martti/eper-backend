defmodule EperBackendTest do
  use ExUnit.Case
  doctest EperBackend

  test "greets the world" do
    assert EperBackend.hello() == :world
  end
end
