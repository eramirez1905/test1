const {calculate_break_time} = require("../../dags/log/curated_data/sql/_rider_transitions");
const fixtures = require("./fixtures/courier_transitions.json");

describe("Break time calculations", () => {
  test("start and end break time", () => {
    fixtures.forEach((row) => {
      expect(calculate_break_time(row.actual)).toEqual(row.expected);
    });
  });
});

