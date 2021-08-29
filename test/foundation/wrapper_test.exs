defmodule FoundationTest.Wrapper do
  use ExUnit.Case

  defmodule FakeWrapper do
    import Foundation.Wrapper

    deftx print(tx) do
      Foundation.get(tx, ["fakewrapper"])
    end
  end

  test "Get/Set" do
    Foundation.DB.start_link(clusterfile: System.get_env("FDB_CLUSTER_FILE"))
    Foundation.trans(Foundation.db(), fn tx -> Foundation.set(tx, ["fakewrapper"], 1) end)

    Foundation.trans(Foundation.db(), fn tx ->
      assert FakeWrapper.print(tx) == 1
    end)

    assert FakeWrapper.print!() == 1
  end
end
