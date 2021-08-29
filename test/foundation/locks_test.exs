defmodule FoundationTest.Locks do
  use ExUnit.Case

  alias Foundation.Locks

  test "Locks.acquire_lock" do
    db = :erlfdb_util.get_test_db(empty: true)
    now = Locks.now()
    later = Locks.now(100)

    Foundation.trans(db, fn tx ->
      assert {:one, later} == Locks.acquire_lock(tx, ["locks", "test"], :one, Locks.now(100))
      assert {:one, later} == Locks.acquire_lock(tx, ["locks", "test"], :two)
      assert {:one, now} == Locks.acquire_lock(tx, ["locks", "test"], :one, now)
      Process.sleep(1_000)
      assert {:two, later} == Locks.acquire_lock(tx, ["locks", "test"], :two, later)
    end)
  end
end
