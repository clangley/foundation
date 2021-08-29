defmodule FoundationTest.Log do
  use ExUnit.Case
  alias Foundation.Log

  test "Bucket Test" do
    db = :erlfdb_util.get_test_db(empty: true)

    ledger_events = [
      {["bank_account", "sally"], {:deposit, 100}},
      {["bank_account", "bob"], {:withdraw, 10}}
    ]

    Foundation.trans(db, fn tx ->
      Enum.each(ledger_events, fn {path, value} ->
        Log.append(tx, path, value)
      end)
    end)

    # Return should be something like this for reference
    # [
    #  {["bank_account", "bob", {:versionstamp, _, _, _}], {:withdraw, 100}},
    #  {["bank_account", "sally", {:versionstamp, _, _, _}], {:deposit, 100}},
    # ]

    # Given that string "bob" is before string "sally", the keys are sorted in FDB and thus returned in that exact order

    Foundation.trans(db, fn tx ->
      [bob, sally] = Log.since(tx, ["bank_account"])
      assert match?({["bank_account", "bob", {:versionstamp, _, _, _}], {:withdraw, 10}}, bob)
      assert match?({["bank_account", "sally", {:versionstamp, _, _, _}], {:deposit, 100}}, sally)

      assert [bob] == Log.before(tx, ["bank_account", "sally"])
    end)
  end

  test "Simple log" do
    db = :erlfdb_util.get_test_db(empty: true)

    Foundation.trans(db, fn tx ->
      Log.append(tx, ["a", 1], 1, version: false)
      Log.append(tx, ["a", 6], 1, version: false)
      Log.append(tx, ["a", 123], 1, version: false)
      Log.append(tx, ["b", 123_023], 1, version: false)
      Log.append(tx, ["c", "a"], 1, version: false)
      Log.append(tx, ["d", true], 1, version: false)
      Log.append(tx, ["e", false], 1, version: false)
    end)

    Foundation.trans(db, fn tx ->
      assert Log.before(tx, ["b"]) == [{["a", 123], 1}, {["a", 6], 1}, {["a", 1], 1}]
      assert Log.since(tx, ["c"]) == [{["c", "a"], 1}]
    end)
  end

  test "Since" do
    db = :erlfdb_util.get_test_db(empty: true)

    Foundation.trans(db, fn tx ->
      Log.append(tx, ["bank_account", "bob"], 100)
      Log.append(tx, ["bank_account", "bob"], 101)
      Log.append(tx, ["bank_account", "bob"], 102)
      Log.append(tx, ["bank_account", "bob"], 103)
      Log.append(tx, ["bank_account", "bob"], 104)
      Log.append(tx, ["bank_account", "bp"], 666)
    end)

    Foundation.trans(db, fn tx ->
      assert length(Log.since(tx, ["bank_account", "bob"])) == 5
      # [{["bank_account", "bob", {:versionstamp, 4737478030, 0, 0}], 100}]
      [{key, v}] = Log.since(tx, ["bank_account", "bob"], limit: 1)
      assert v == 100

      assert Enum.map(Log.since(tx, key, limit: 100), &elem(&1, 1)) == [
               101,
               102,
               103,
               104
             ]
    end)
  end
end
