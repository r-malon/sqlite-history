package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	_ "modernc.org/sqlite"
)

// ColumnInfo represents a column's name and type
type ColumnInfo struct {
	Name string
	Type string
}

// SQLiteHistory provides methods for configuring history tracking
type SQLiteHistory struct {
	db *sql.DB
}

// New creates a new SQLiteHistory instance
func New(db *sql.DB) *SQLiteHistory {
	return &SQLiteHistory{db: db}
}

// ConfigureHistory sets up history tracking for a table
func (sh *SQLiteHistory) ConfigureHistory(table string) error {
	// Get table schema
	columns, err := sh.getTableColumnsAndTypes(table)
	if err != nil {
		return fmt.Errorf("failed to get table schema: %w", err)
	}

	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}

	// Begin transaction
	tx, err := sh.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create history table
	historyTableSQL := sh.generateHistoryTableSQL(table, columns)
	if _, err := tx.Exec(historyTableSQL); err != nil {
		return fmt.Errorf("failed to create history table: %w", err)
	}

	// Create triggers
	triggersSQL := sh.generateTriggersSQL(table, columnNames)
	if _, err := tx.Exec(triggersSQL); err != nil {
		return fmt.Errorf("failed to create triggers: %w", err)
	}

	// Backfill history table
	backfillSQL := sh.generateBackfillSQL(table, columnNames)
	if _, err := tx.Exec(backfillSQL); err != nil {
		return fmt.Errorf("failed to backfill history: %w", err)
	}

	return tx.Commit()
}

// getTableColumnsAndTypes retrieves column information for a table
func (sh *SQLiteHistory) getTableColumnsAndTypes(table string) ([]ColumnInfo, error) {
	query := fmt.Sprintf("PRAGMA table_info([%s]);", table)
	rows, err := sh.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var defaultValue interface{}

		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		columns = append(columns, ColumnInfo{Name: name, Type: dataType})
	}

	return columns, rows.Err()
}

// generateHistoryTableSQL creates SQL for the history table
func (sh *SQLiteHistory) generateHistoryTableSQL(table string, columns []ColumnInfo) string {
	var columnDefs []string
	for _, col := range columns {
		columnDefs = append(columnDefs, fmt.Sprintf("    %s %s", escapeSQLite(col.Name), col.Type))
	}

	return fmt.Sprintf(`
CREATE TABLE _%s_history (
    _rowid INTEGER,
%s,
    _version INTEGER,
    _updated INTEGER,
    _mask INTEGER
);
CREATE INDEX idx_%s_history_rowid ON _%s_history (_rowid);
`, table, strings.Join(columnDefs, ",\n"), table, table)
}

// generateTriggersSQL creates SQL for insert, update, and delete triggers
func (sh *SQLiteHistory) generateTriggersSQL(table string, columns []string) string {
	escapedColumns := make([]string, len(columns))
	newColumnValues := make([]string, len(columns))
	oldColumnValues := make([]string, len(columns))

	for i, col := range columns {
		escaped := escapeSQLite(col)
		escapedColumns[i] = escaped
		newColumnValues[i] = "new." + escaped
		oldColumnValues[i] = "old." + escaped
	}

	columnNames := strings.Join(escapedColumns, ", ")
	newValues := strings.Join(newColumnValues, ", ")
	oldValues := strings.Join(oldColumnValues, ", ")
	mask := (1 << len(columns)) - 1

	// Insert trigger
	insertTrigger := fmt.Sprintf(`
CREATE TRIGGER %s_insert_history
AFTER INSERT ON %s
BEGIN
    INSERT INTO _%s_history (_rowid, %s, _version, _updated, _mask)
    VALUES (new.rowid, %s, 1, cast((julianday('now') - 2440587.5) * 86400 * 1000 as integer), %d);
END;
`, table, table, table, columnNames, newValues, mask)

	// Update trigger
	var updateColumns []string
	for _, col := range columns {
		escaped := escapeSQLite(col)
		updateColumns = append(updateColumns, fmt.Sprintf(`
        CASE WHEN old.%s IS NOT new.%s then new.%s else null end`, escaped, escaped, escaped))
	}
	updateColumnsSQL := strings.Join(updateColumns, ",")

	var maskParts []string
	for i, col := range columns {
		escaped := escapeSQLite(col)
		base := 1 << i
		maskParts = append(maskParts, fmt.Sprintf("(CASE WHEN old.%s IS NOT new.%s then %d else 0 end)", escaped, escaped, base))
	}
	maskSQL := strings.Join(maskParts, " + ")

	var whereParts []string
	for _, col := range columns {
		escaped := escapeSQLite(col)
		whereParts = append(whereParts, fmt.Sprintf("old.%s IS NOT new.%s", escaped, escaped))
	}
	whereSQL := strings.Join(whereParts, " or ")

	updateTrigger := fmt.Sprintf(`
CREATE TRIGGER %s_update_history
AFTER UPDATE ON %s
FOR EACH ROW
BEGIN
    INSERT INTO _%s_history (_rowid, %s, _version, _updated, _mask)
    SELECT old.rowid, %s,
        (SELECT MAX(_version) FROM _%s_history WHERE _rowid = old.rowid) + 1,
        cast((julianday('now') - 2440587.5) * 86400 * 1000 as integer),
        %s
    WHERE %s;
END;
`, table, table, table, columnNames, updateColumnsSQL, table, maskSQL, whereSQL)

	// Delete trigger
	deleteTrigger := fmt.Sprintf(`
CREATE TRIGGER %s_delete_history
AFTER DELETE ON %s
BEGIN
    INSERT INTO _%s_history (_rowid, %s, _version, _updated, _mask)
    VALUES (
        old.rowid,
        %s,
        (SELECT COALESCE(MAX(_version), 0) from _%s_history WHERE _rowid = old.rowid) + 1,
        cast((julianday('now') - 2440587.5) * 86400 * 1000 as integer),
        -1
    );
END;
`, table, table, table, columnNames, oldValues, table)

	return insertTrigger + updateTrigger + deleteTrigger
}

// generateBackfillSQL creates SQL to populate history with existing data
func (sh *SQLiteHistory) generateBackfillSQL(table string, columns []string) string {
	escapedColumns := make([]string, len(columns))
	for i, col := range columns {
		escapedColumns[i] = escapeSQLite(col)
	}
	columnNames := strings.Join(escapedColumns, ", ")
	mask := (1 << len(columns)) - 1

	return fmt.Sprintf(`
INSERT INTO _%s_history (_rowid, %s, _version, _updated, _mask)
SELECT rowid, %s, 1, cast((julianday('now') - 2440587.5) * 86400 * 1000 as integer), %d
FROM %s;
`, table, columnNames, columnNames, mask, table)
}

// getAllRegularTables returns all non-system tables
func (sh *SQLiteHistory) getAllRegularTables() ([]string, error) {
	// Get FTS and system tables to exclude
	hiddenTablesQuery := `
		SELECT name FROM sqlite_master
		WHERE type = 'table'
		AND (
			sql LIKE '%VIRTUAL TABLE%USING FTS%'
		) OR name IN ('sqlite_sequence', 'sqlite_stat1', 'sqlite_stat2', 'sqlite_stat3', 'sqlite_stat4')
	`

	rows, err := sh.db.Query(hiddenTablesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hiddenTables := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		hiddenTables[name] = true
	}

	// Get all table names
	allTablesQuery := "SELECT name FROM sqlite_master WHERE type='table';"
	rows, err = sh.db.Query(allTablesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var regularTables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		// Skip if it's a hidden table or starts with a hidden table name
		shouldSkip := false
		for hiddenTable := range hiddenTables {
			if strings.HasPrefix(name, hiddenTable) {
				shouldSkip = true
				break
			}
		}

		if !shouldSkip {
			regularTables = append(regularTables, name)
		}
	}

	return regularTables, rows.Err()
}

// tableExists checks if a table exists in the database
func (sh *SQLiteHistory) tableExists(tableName string) (bool, error) {
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name = ?;"
	var name string
	err := sh.db.QueryRow(query, tableName).Scan(&name)
	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// configureTriggers sets up history for multiple tables
func (sh *SQLiteHistory) configureTriggers(tables []string) error {
	for _, table := range tables {
		// Skip history tables
		if strings.HasPrefix(table, "_") && strings.HasSuffix(table, "_history") {
			continue
		}

		// Check if history table already exists
		historyTableName := fmt.Sprintf("_%s_history", table)
		exists, err := sh.tableExists(historyTableName)
		if err != nil {
			return fmt.Errorf("failed to check if history table exists: %w", err)
		}

		if exists {
			fmt.Printf("History table %s already exists - skipping.\n", historyTableName)
			continue
		}

		if err := sh.ConfigureHistory(table); err != nil {
			return fmt.Errorf("failed to configure history for table %s: %w", table, err)
		}

		fmt.Printf("Configured history for table: %s\n", table)
	}
	return nil
}

// SQLite reserved words
var reservedWords = map[string]bool{
	"abort": true, "action": true, "add": true, "after": true, "all": true, "alter": true,
	"analyze": true, "and": true, "as": true, "asc": true, "attach": true, "autoincrement": true,
	"before": true, "begin": true, "between": true, "by": true, "cascade": true, "case": true,
	"cast": true, "check": true, "collate": true, "column": true, "commit": true, "conflict": true,
	"constraint": true, "create": true, "cross": true, "current_date": true, "current_time": true,
	"current_timestamp": true, "database": true, "default": true, "deferrable": true,
	"deferred": true, "delete": true, "desc": true, "detach": true, "distinct": true, "drop": true,
	"each": true, "else": true, "end": true, "escape": true, "except": true, "exclusive": true,
	"exists": true, "explain": true, "fail": true, "for": true, "foreign": true, "from": true,
	"full": true, "glob": true, "group": true, "having": true, "if": true, "ignore": true,
	"immediate": true, "in": true, "index": true, "indexed": true, "initially": true, "inner": true,
	"insert": true, "instead": true, "intersect": true, "into": true, "is": true, "isnull": true,
	"join": true, "key": true, "left": true, "like": true, "limit": true, "match": true,
	"natural": true, "no": true, "not": true, "notnull": true, "null": true, "of": true,
	"offset": true, "on": true, "or": true, "order": true, "outer": true, "plan": true,
	"pragma": true, "primary": true, "query": true, "raise": true, "recursive": true,
	"references": true, "regexp": true, "reindex": true, "release": true, "rename": true,
	"replace": true, "restrict": true, "right": true, "rollback": true, "row": true,
	"savepoint": true, "select": true, "set": true, "table": true, "temp": true, "temporary": true,
	"then": true, "to": true, "transaction": true, "trigger": true, "union": true, "unique": true,
	"update": true, "using": true, "vacuum": true, "values": true, "view": true, "virtual": true,
	"when": true, "where": true, "with": true, "without": true,
}

var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// escapeSQLite escapes SQLite identifiers
func escapeSQLite(s string) string {
	if validIdentifier.MatchString(s) && !reservedWords[strings.ToLower(s)] {
		return s
	}
	return fmt.Sprintf("[%s]", s)
}

// CLI functionality
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s [database_path] [tables... | -A | --all]\n", os.Args[0])
		os.Exit(1)
	}

	dbPath := os.Args[1]
	var tables []string
	var useAll bool

	// Parse arguments
	for i := 2; i < len(os.Args); i++ {
		if os.Args[i] == "-A" || os.Args[i] == "--all" {
			useAll = true
		} else {
			tables = append(tables, os.Args[i])
		}
	}

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("Database file does not exist: %s", dbPath)
	}

	// Open database
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	sh := New(db)

	// Get tables to process
	if useAll {
		tables, err = sh.getAllRegularTables()
		if err != nil {
			log.Fatalf("Failed to get table list: %v", err)
		}
	} else if len(tables) == 0 {
		log.Fatal("No tables provided. Please provide table names or use --all flag.")
	}

	// Validate that all specified tables exist
	if !useAll {
		allTables, err := sh.getAllRegularTables()
		if err != nil {
			log.Fatalf("Failed to get table list for validation: %v", err)
		}

		tableMap := make(map[string]bool)
		for _, table := range allTables {
			tableMap[table] = true
		}

		var missingTables []string
		for _, table := range tables {
			if !tableMap[table] {
				missingTables = append(missingTables, table)
			}
		}

		if len(missingTables) > 0 {
			log.Fatalf("The following tables do not exist: %s", strings.Join(missingTables, ", "))
		}
	}

	// Configure history for tables
	if err := sh.configureTriggers(tables); err != nil {
		log.Fatalf("Failed to configure triggers: %v", err)
	}
}
