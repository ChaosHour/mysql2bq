package pipeline

import (
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestMySQLTypeToBigQueryType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bigquery.FieldType
	}{
		// Integer types
		{"tinyint", "tinyint", bigquery.IntegerFieldType},
		{"tinyint unsigned", "tinyint unsigned", bigquery.IntegerFieldType},
		{"smallint", "smallint", bigquery.IntegerFieldType},
		{"mediumint", "mediumint", bigquery.IntegerFieldType},
		{"int", "int", bigquery.IntegerFieldType},
		{"int(11)", "int(11)", bigquery.IntegerFieldType},
		{"integer", "integer", bigquery.IntegerFieldType},
		{"bigint", "bigint", bigquery.IntegerFieldType},

		// Boolean
		{"tinyint(1)", "tinyint(1)", bigquery.BooleanFieldType},
		{"boolean", "boolean", bigquery.BooleanFieldType},
		{"bool", "bool", bigquery.BooleanFieldType},

		// Floating point
		{"float", "float", bigquery.FloatFieldType},
		{"float(7,4)", "float(7,4)", bigquery.FloatFieldType},
		{"double", "double", bigquery.FloatFieldType},
		{"double precision", "double precision", bigquery.FloatFieldType},
		{"real", "real", bigquery.FloatFieldType},

		// Decimal
		{"decimal", "decimal", bigquery.NumericFieldType},
		{"decimal(10,2)", "decimal(10,2)", bigquery.NumericFieldType},
		{"numeric", "numeric", bigquery.NumericFieldType},
		{"numeric(15,5)", "numeric(15,5)", bigquery.NumericFieldType},

		// String types
		{"char", "char", bigquery.StringFieldType},
		{"char(10)", "char(10)", bigquery.StringFieldType},
		{"varchar", "varchar", bigquery.StringFieldType},
		{"varchar(255)", "varchar(255)", bigquery.StringFieldType},
		{"text", "text", bigquery.StringFieldType},
		{"tinytext", "tinytext", bigquery.StringFieldType},
		{"mediumtext", "mediumtext", bigquery.StringFieldType},
		{"longtext", "longtext", bigquery.StringFieldType},
		{"enum", "enum", bigquery.StringFieldType},
		{"set", "set", bigquery.StringFieldType},

		// Binary types
		{"binary", "binary", bigquery.BytesFieldType},
		{"binary(16)", "binary(16)", bigquery.BytesFieldType},
		{"varbinary", "varbinary", bigquery.BytesFieldType},
		{"blob", "blob", bigquery.BytesFieldType},
		{"tinyblob", "tinyblob", bigquery.BytesFieldType},
		{"mediumblob", "mediumblob", bigquery.BytesFieldType},
		{"longblob", "longblob", bigquery.BytesFieldType},

		// Date/Time types
		{"date", "date", bigquery.DateFieldType},
		{"datetime", "datetime", bigquery.TimestampFieldType},
		{"datetime(6)", "datetime(6)", bigquery.TimestampFieldType},
		{"timestamp", "timestamp", bigquery.TimestampFieldType},
		{"timestamp(6)", "timestamp(6)", bigquery.TimestampFieldType},
		{"time", "time", bigquery.TimeFieldType},
		{"year", "year", bigquery.IntegerFieldType},

		// Special types
		{"json", "json", bigquery.JSONFieldType},
		{"uuid", "uuid", bigquery.StringFieldType},
		{"binary(16) uuid", "binary(16)", bigquery.BytesFieldType},

		// Unknown types default to string
		{"unknown", "unknown", bigquery.StringFieldType},
		{"custom_type", "custom_type", bigquery.StringFieldType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MySQLTypeToBigQueryType(tt.input)
			if result != tt.expected {
				t.Errorf("MySQLTypeToBigQueryType(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestMySQLColumnNameToBigQuery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple name", "userId", "userId"},
		{"camelCase", "createdAt", "createdAt"},
		{"PascalCase", "UserName", "UserName"},
		{"snake_case", "user_name", "user_name"},
		{"with numbers", "user123", "user123"},
		{"with underscore", "user_id", "user_id"},
		{"empty string", "", ""},
		{"special chars", "user-name", "user-name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MySQLColumnNameToBigQuery(tt.input)
			if result != tt.expected {
				t.Errorf("MySQLColumnNameToBigQuery(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestMySQLTypeToBigQueryTypeCaseInsensitive(t *testing.T) {
	tests := []struct {
		name  string
		input string
		base  string
	}{
		{"uppercase", "INT", "int"},
		{"mixed case", "InT", "int"},
		{"lowercase", "int", "int"},
		{"with spaces", " INT ", "int"},
		{"varchar uppercase", "VARCHAR", "varchar"},
		{"datetime mixed", "DateTime", "datetime"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MySQLTypeToBigQueryType(tt.input)
			expected := MySQLTypeToBigQueryType(tt.base) // Compare to the base lowercase version
			if result != expected {
				t.Errorf("MySQLTypeToBigQueryType(%q) = %v, want %v (case insensitive)", tt.input, result, expected)
			}
		})
	}
}
