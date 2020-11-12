const calculate_break_time = (list) => {
  let breakStartedAt = null, busyEndedAt = null, previous;
  Object.keys(list)
    .reverse()
    .forEach((index) => {
      let v = list[index];
      if (!breakStartedAt && [
        'busy_started_at',
        'working'
      ].includes(v.state)) {
        breakStartedAt = previous;
      }
      previous = v.created_at;
      if (!busyEndedAt && breakStartedAt) {
        busyEndedAt = previous;
      }
    });
  return [
    {state: 'break_started_at', created_at: breakStartedAt},
    {state: 'break_ended_at', created_at: busyEndedAt}
  ];
}

// {% if False %}
module.exports = {
  calculate_break_time
}
// {% endif %}
