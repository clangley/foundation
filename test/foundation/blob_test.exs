defmodule FoundationTest.Blob do
  use ExUnit.Case
  alias Foundation.Blob

  test "Blob Test" do
    db = :erlfdb_util.get_test_db(empty: true)
    blob = Blob.new(db, ["s3", "data"], "0123456789", 5)

    assert blob.path == ["s3", "data"]

    assert blob.size == 10

    assert blob.count == 2
    Blob.upload(blob)

    assert Blob.get(db, blob.path) == [
             {["s3", "data", "index", 0], "01234"},
             {["s3", "data", "index", 1], "56789"}
           ]
  end
end
