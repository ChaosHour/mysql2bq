package pipeline

import (
	"strings"

	"cloud.google.com/go/bigquery"
)

// MySQLColumnNameToBigQuery preserves the exact casing of MySQL column names.
// BigQuery is case-sensitive, so we maintain the original casing (e.g., camelCase).
// This ensures consistency between MySQL and BigQuery schemas.
func MySQLColumnNameToBigQuery(mysqlColumnName string) string {
	// BigQuery column names are case-sensitive, so return as-is
	// MySQL column names are typically case-insensitive on most systems,
	// but we preserve the casing for consistency with BigQuery
	return mysqlColumnName
}

// MySQLTypeToBigQueryType maps MySQL column type strings to BigQuery field types.
// This provides a dynamic, automatic mapping for common MySQL types.
// For more complex types or precision handling, this can be extended.
func MySQLTypeToBigQueryType(mysqlType string) bigquery.FieldType {
	// Normalize to lowercase for case-insensitive matching
	mysqlType = strings.ToLower(strings.TrimSpace(mysqlType))

	// Handle specific cases first
	if mysqlType == "tinyint(1)" || mysqlType == "boolean" || mysqlType == "bool" {
		return bigquery.BooleanFieldType
	}

	// Integer types
	if strings.HasPrefix(mysqlType, "tinyint") {
		return bigquery.IntegerFieldType
	}
	if strings.HasPrefix(mysqlType, "smallint") {
		return bigquery.IntegerFieldType
	}
	if strings.HasPrefix(mysqlType, "mediumint") {
		return bigquery.IntegerFieldType
	}
	if strings.HasPrefix(mysqlType, "int") || strings.HasPrefix(mysqlType, "integer") {
		return bigquery.IntegerFieldType
	}
	if strings.HasPrefix(mysqlType, "bigint") {
		return bigquery.IntegerFieldType
	}

	// Floating point types
	if strings.HasPrefix(mysqlType, "float") {
		return bigquery.FloatFieldType
	}
	if strings.HasPrefix(mysqlType, "double") || strings.HasPrefix(mysqlType, "real") {
		return bigquery.FloatFieldType
	}

	// Decimal types - BigQuery supports NUMERIC for fixed precision
	if strings.HasPrefix(mysqlType, "decimal") || strings.HasPrefix(mysqlType, "numeric") {
		return bigquery.NumericFieldType
	}

	// String types
	if strings.HasPrefix(mysqlType, "char") ||
		strings.HasPrefix(mysqlType, "varchar") ||
		strings.HasPrefix(mysqlType, "text") ||
		strings.HasPrefix(mysqlType, "tinytext") ||
		strings.HasPrefix(mysqlType, "mediumtext") ||
		strings.HasPrefix(mysqlType, "longtext") ||
		strings.HasPrefix(mysqlType, "enum") ||
		strings.HasPrefix(mysqlType, "set") {
		return bigquery.StringFieldType
	}

	// Binary types
	if strings.HasPrefix(mysqlType, "binary") ||
		strings.HasPrefix(mysqlType, "varbinary") ||
		strings.HasPrefix(mysqlType, "blob") ||
		strings.HasPrefix(mysqlType, "tinyblob") ||
		strings.HasPrefix(mysqlType, "mediumblob") ||
		strings.HasPrefix(mysqlType, "longblob") {
		return bigquery.BytesFieldType
	}

	// Date and time types
	if strings.HasPrefix(mysqlType, "datetime") || strings.HasPrefix(mysqlType, "timestamp") {
		return bigquery.TimestampFieldType
	}
	if strings.HasPrefix(mysqlType, "date") {
		return bigquery.DateFieldType
	}
	if strings.HasPrefix(mysqlType, "time") {
		return bigquery.TimeFieldType
	}
	if strings.HasPrefix(mysqlType, "year") {
		return bigquery.IntegerFieldType // Year as integer
	}

	// JSON type
	if mysqlType == "json" {
		return bigquery.JSONFieldType
	}

	// UUID and other special types - treat as string
	if strings.Contains(mysqlType, "uuid") {
		return bigquery.StringFieldType
	}

	// Default to string for unknown types
	return bigquery.StringFieldType
}

// Example usage when creating BigQuery schema:
// Assuming you have MySQL column info with name and type
// mysqlColumnName := "userId"  // camelCase from MySQL
// mysqlType := "int(11)"
//
// bqName := MySQLColumnNameToBigQuery(mysqlColumnName)  // preserves "userId"
// bqType := MySQLTypeToBigQueryType(mysqlType)          // returns bigquery.IntegerFieldType
//
// field := &bigquery.FieldSchema{
//     Name: bqName,
//     Type: bqType,
// }
