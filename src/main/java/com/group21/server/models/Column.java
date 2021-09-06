package com.group21.server.models;

public class Column {
    private String columnName;
    private DataType columnType;
    private Constraint constraint;
    private String foreignKeyTable;
    private String foreignKeyColumnName;
    private Integer columnPosition;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public DataType getColumnType() {
        return columnType;
    }

    public void setColumnType(DataType columnType) {
        this.columnType = columnType;
    }

    public Constraint getConstraint() {
        return constraint;
    }

    public void setConstraint(Constraint constraint) {
        this.constraint = constraint;
    }

    public String getForeignKeyTable() {
        return foreignKeyTable;
    }

    public void setForeignKeyTable(String foreignKeyTable) {
        this.foreignKeyTable = foreignKeyTable;
    }

    public String getForeignKeyColumnName() {
        return foreignKeyColumnName;
    }

    public void setForeignKeyColumnName(String foreignKeyColumnName) {
        this.foreignKeyColumnName = foreignKeyColumnName;
    }

    public Integer getColumnPosition() {
        return columnPosition;
    }

    public void setColumnPosition(Integer columnPosition) {
        this.columnPosition = columnPosition;
    }
}
