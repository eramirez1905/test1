const parse_dynamic_pricing_sessions = (request_json, response_json, version) => {
  const request = JSON.parse(request_json);
  const response = JSON.parse(response_json);
  
  let customer = {
    id: null,
    user_id: null,
    session: {
      id: null,
      timestamp: null
    },
    latitude: null,
    longitude: null,
    apply_abtest: null,
    variant: null,
    tag: null,
  };
  
  if (version === 'V1') {
    customer.id = request.id;
    customer.user_id = request.user_id;
    customer.session.id = request.session_id;
    customer.latitude = request.latitude;
    customer.longitude = request.longitude;
    customer.apply_abtest = response.apply_abtest;
  } else if (version === 'V2') {
    customer = {...customer, ...request};
    customer.variant = response.variant;
    customer.tag = response.tag;
    if (/^[0-9]{10}$/.test(customer.session.timestamp)) {
      customer.session.timestamp = new Date(customer.session.timestamp * 1000);
    } else {
      customer.session.timestamp = null;
    }
  } else {
    throw `version "${version}" not found`;
  }
  return customer;
}

// {% if False %}
module.exports = {
  parse_dynamic_pricing_sessions
}
// {% endif %}
