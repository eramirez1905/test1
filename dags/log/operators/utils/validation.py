from datetime import datetime, date

import cerberus as c
import numpy as np


def coerce_to_datetime(x: str) -> datetime:
    """Parse datetime string and return date object."""
    return datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S')


def coerce_to_date(x: str) -> date:
    """Parse date string and return date object."""
    # ref: https://github.com/python/cpython/blob/cfd735ea28a2b985598236f955c72c3f0e82e01d/Lib/datetime.py#L260
    # plus added string length check
    if len(x) != 10:
        raise ValueError(f'Invalid string length: must be 10')

    year = int(x[0:4])
    if x[4] != '-':
        raise ValueError('Invalid date separator: %s' % x[4])

    month = int(x[5:7])

    if x[7] != '-':
        raise ValueError('Invalid date separator')

    day = int(x[8:10])

    return date(year, month, day)


def normalize_schema(schema):
    """Parse date & datetime rules and add coercion strategy."""
    rules = ['min', 'max']

    for key, value in schema.items():
        if value['type'] == 'datetime':
            schema[key]['coerce'] = coerce_to_datetime
            for rule in rules:
                if rule in schema[key]:
                    schema[key][rule] = coerce_to_datetime(schema[key][rule])
        if value['type'] == 'date':
            schema[key]['coerce'] = coerce_to_date
            for rule in rules:
                if rule in schema[key]:
                    schema[key][rule] = coerce_to_date(schema[key][rule])

    return schema


def validate_schema(data, schema):
    """Validate the schema of a data frame and return a validation summary."""
    schema = normalize_schema(schema)
    schema_validator = c.Validator(schema)
    schema_validator.allow_unknown = True

    validations = list()
    errors = list()
    for row in data.itertuples():
        row_dict = row._asdict()
        row_validation = schema_validator.validate(row_dict)
        errors.append(schema_validator.errors)
        validations.append(row_validation)

    rows_valid = np.count_nonzero(validations)
    rows_not_valid = len(validations) - rows_valid

    # concat errors to 'column_name: condition'
    errors_str = list()
    for error in errors:
        for key, value in error.items():
            key_value = str(key) + ': ' + str(value)
            errors_str.append(key_value)

    # concat unique errors and their occurrences to 'error: x times'
    error, count = np.unique(errors_str, return_counts=True)
    error_counts = [f"{e} ({c} times)" for (e, c) in zip(error, count)]

    validation = {'is_valid': rows_not_valid == 0,
                  'has_been_validated': len(validations) > 0,
                  'rows': {'is_valid': rows_valid,
                           'is_not_valid': rows_not_valid},
                  'summary': error_counts}

    return validation


def summarize_validation(validation):
    """Summarizes validation result to a convenient string"""
    schema_validation = validation['schema']
    primary_key_validation = validation['primary_key']

    summary_str = ""

    if not primary_key_validation:
        summary_str += 'Primary key validation failed.\n'
    else:
        summary_str += 'Primary key validation successful.\n'

    if not schema_validation['is_valid']:
        rows_not_valid = schema_validation['rows']['is_not_valid']
        summary_str += f'There were schema validation errors in {rows_not_valid} row(s):\n'
        # show first 100 errors
        for s in schema_validation['summary'][:100]:
            summary_str += s + '\n'
    else:
        rows_valid = schema_validation['rows']['is_valid']
        summary_str += f'Schema validation successful for {rows_valid} row(s).\n'

    return summary_str


def validate_primary_key(data, primary_key):
    """Validate primary key integrity of a data frame"""
    try:
        data.set_index(primary_key, verify_integrity=True)
    except ValueError:
        return False
    except KeyError:
        return False
    return True


def validate_data(data, validation):
    """"Validate primary key and schema and return a validation summary."""
    primary_key = validation['primary_key']
    primary_key_validation = validate_primary_key(data, primary_key)

    schema = validation['schema']
    schema_validation = validate_schema(data, schema)

    return {
        'is_valid': primary_key_validation and schema_validation['is_valid'],
        'primary_key': primary_key_validation,
        'schema': schema_validation
    }
