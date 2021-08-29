defmodule FoundationTest do
  use ExUnit.Case

  test "Get/Set" do
    db = :erlfdb_util.get_test_db(empty: true)

    Foundation.trans(db, fn tx ->
      Foundation.set(tx, ["hello"], "foo") |> Foundation.wait()
      assert Foundation.get(tx, ["hello"]) == "foo"
    end)
  end

  test "Get/Set versionstamps" do
    db = :erlfdb_util.get_test_db(empty: true)

    Foundation.trans(db, fn tx ->
      Foundation.set(tx, ["hello", Foundation.versionstamp(tx)], "This is a new message")
    end)

    Foundation.trans(db, fn tx ->
      [head | _t] = Foundation.get_range_startswith(tx, ["hello"])
      assert match?({["hello", {:versionstamp, _, _, 0}], "This is a new message"}, head)
    end)
  end
end
