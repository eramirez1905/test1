const {parse_dynamic_pricing_sessions} = require("../../dags/log/curated_data/sql/dynamic_pricing_user_sessions");

const fixtures = {
  happy_scenario: require("./fixtures/dynamic_pricing_request_response_logs.json"),
  wrong_timestamp: require("./fixtures/dynamic_pricing_request_response_logs_wrong_timestamp.json")
}

describe("Dynamic Pricing user sessions", () => {
  test.each([
    ["happy_scenario"],
    ["wrong_timestamp"],
  ])("test %s", (use_case) => {
    const actual = fixtures[use_case].actual;
    const expected = fixtures[use_case].expected;
    const request = JSON.stringify(JSON.parse(actual.request).customer);
    const response = JSON.stringify(JSON.parse(actual.response).customer);

    const user_sessions = parse_dynamic_pricing_sessions(request, response, actual.version)
    if (user_sessions["session"]["timestamp"] !== null) {
      expect(user_sessions["session"]["timestamp"]).toBeInstanceOf(Date)
      user_sessions["session"]["timestamp"] = user_sessions["session"]["timestamp"].toISOString();
    }

    expect(user_sessions).toEqual(expected);
  });
});
