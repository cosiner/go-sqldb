// Package sqldb helps create tables from Go structures.
//
// Field tag format: `sqldb:"key[:value] key[:value]..."`. Available keys:
//   table: table name
//   col: column name, col:- to skip.
//   type: char, text and Go builtin types: string/bool/int/uint/int8...
//   precision: for string and char type, it's the 'length', such as precision:100,
//              for float and double it's 'precision, exact', such as precision: 32,5.
//   dbtype: the final database type, it will override type and precision key
//   pk: primary key
//   autoincr: auto increament
//   notnull: not null
//   default: default value, '-' to disable default
//   unique: unique constraint name or empty
//   fk: foreign key: TABLE.COLUMN
package sqldb
