const fs = require('fs');
const {execFileSync} = require('child_process');
const assert = require('assert');

describe('database', () => {
  before(() => {
      try {
        fs.unlinkSync('test.db')
      } catch (_) { }
  })

  function run_script(commands) {
    const output = execFileSync("./target/debug/db_tutorial", {
        input: commands.join('\n'),
        env: { RUST_BACKTRACE: 1 }
    })
    return output.toString().split("\n")
  }

  it('inserts and retreives a row', () => {
    const result = run_script([
      "insert 1 user1 person1@example.com",
      "select",
      ".exit",
    ])
    assert.deepEqual(result, [
      "db > Executed.",
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > ",
    ])
  })

  // TODO: part 5
  // it('keeps data after closing connection', () => {
  //   const result1 = run_script([
  //     "insert 1 user1 person1@example.com",
  //     ".exit",
  //   ])
  //   assert.deepEqual(result1, [
  //     "db > Executed.",
  //     "db > ",
  //   ])

  //   const result2 = run_script([
  //     "select",
  //     ".exit",
  //   ])
  //   assert.deepEqual(result2, [
  //     "db > (1, user1, person1@example.com)",
  //     "Executed.",
  //     "db > ",
  //   ])
  // })

  it('prints error message when table is full', () => {
    const script = Array.from(Array(1400).keys())
        .map(i => `insert ${i + 1} user${i + 1} person${i + 1}@example.com`)
    script.push(".exit")
    const result = run_script(script)
    assert.deepEqual(result[result.length-2], 'db > Error: Table full.')
  })

  it('allows inserting strings that are the maximum length', () => {
    const long_username = 'a'.repeat(32)
    const long_email = 'a'.repeat(256)
    const script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit",
    ]
    const result = run_script(script)
    assert.deepEqual(result, [
      "db > Executed.",
      "db > (1, #{long_username}, #{long_email})",
      "Executed.",
      "db > ",
    ])
  })

  it('prints error message if strings are too long', () => {
    const long_username = 'a'.repeat(33)
    const long_email = 'a'.repeat(256)
    const script = [
      `insert 1 ${long_username} ${long_email}`,
      "select",
      ".exit",
    ]
    const result = run_script(script)
    assert.deepEqual(result, [
      "db > String is too long.",
      "db > Executed.",
      "db > ",
    ])
  });

  it('prints an error message if id is negative', () => {
    const script = [
      "insert -1 cstack foo@bar.com",
      "select",
      ".exit",
    ]
    const result = run_script(script)
    assert.deepEqual(result, [
      "db > ID must be positive.",
      "db > Executed.",
      "db > ",
    ])
  })
})