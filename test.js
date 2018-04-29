const fs = require("fs");
const { execFileSync } = require("child_process");
const assert = require("assert");

describe("database", () => {
  beforeEach(() => {
    try {
      fs.unlinkSync("./test.db");
    } catch (_) {}
  });

  function run_script(commands) {
    const output = execFileSync("./target/debug/db_tutorial", ["./test.db"], {
      input: commands.join("\n"),
      env: { RUST_BACKTRACE: 1 }
    });
    return output.toString().split("\n");
  }

  it("inserts and retreives a row", () => {
    const result = run_script([
      "insert 1 user1 person1@example.com",
      "select",
      ".exit"
    ]);
    assert.deepEqual(result, [
      "db > Executed.",
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > "
    ]);
  });

  it("keeps data after closing connection", () => {
    const result1 = run_script(["insert 1 user1 person1@example.com", ".exit"]);
    assert.deepEqual(result1, ["db > Executed.", "db > "]);

    const result2 = run_script(["select", ".exit"]);
    assert.deepEqual(result2, [
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > "
    ]);
  });

  it("prints error message when table is full", () => {
    const script = Array.from(Array(1400).keys()).map(
      i => `insert ${i + 1} user${i + 1} person${i + 1}@example.com`
    );
    script.push(".exit");
    const result = run_script(script);
    assert.deepEqual(result[result.length - 2], "db > Error: Table full.");
  });

  it("allows inserting strings that are the maximum length", () => {
    const long_username = "a".repeat(32);
    const long_email = "a".repeat(256);
    const script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit"
    ];
    const result = run_script(script);
    assert.deepEqual(result, [
      "db > Executed.",
      "db > (1, #{long_username}, #{long_email})",
      "Executed.",
      "db > "
    ]);
  });

  it("prints error message if strings are too long", () => {
    const long_username = "a".repeat(33);
    const long_email = "a".repeat(256);
    const script = [
      `insert 1 ${long_username} ${long_email}`,
      "select",
      ".exit"
    ];
    const result = run_script(script);
    assert.deepEqual(result, [
      "db > String is too long.",
      "db > Executed.",
      "db > "
    ]);
  });

  it("prints an error message if id is negative", () => {
    const script = ["insert -1 cstack foo@bar.com", "select", ".exit"];
    const result = run_script(script);
    assert.deepEqual(result, [
      "db > ID must be positive.",
      "db > Executed.",
      "db > "
    ]);
  });

  it("prints constants", () => {
    const script = [".constants", ".exit"];
    const result = run_script(script);
    assert.deepEqual(result, [
      "db > Constants:",
      "ROW_SIZE: 293",
      "COMMON_NODE_HEADER_SIZE: 6",
      "LEAF_NODE_HEADER_SIZE: 10",
      "LEAF_NODE_CELL_SIZE: 297",
      "LEAF_NODE_SPACE_FOR_CELLS: 4086",
      "LEAF_NODE_MAX_CELLS: 13",
      "db > "
    ]);
  });

  it("allows printing out the structure of a one-node btree", () => {
    const script = [3, 1, 2].map(
      i => `insert ${i} user${i} person${i}@example.com`
    );
    script.push(".btree");
    script.push(".exit");
    const result = run_script(script);
    assert.deepEqual(result, [
      "db > Executed.",
      "db > Executed.",
      "db > Executed.",
      "db > Tree:",
      "leaf (size 3)",
      "  - 0 : 1",
      "  - 1 : 2",
      "  - 2 : 3",
      "db > "
    ]);
  });

  it('prints an error message if there is a duplicate id', () => {
    const script = [
      "insert 1 user1 person1@example.com",
      "insert 1 user1 person1@example.com",
      "select",
      ".exit"
    ];
    const result = run_script(script);
    assert.deepEqual(result, [
      "db > Executed.",
      "db > Error: Duplicate key.",
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > "
    ]);
  });
});
