use anyhow::{bail, Context, Result};
use sqlite_starter_rust::{
    sql_parser::{AggregateFunction, Expr, SQLParser, SelectStmt, Stmt},
    SQLiteDB,
};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let db = SQLiteDB::new(&args[1])?;
            println!("database page size: {}", db.get_page_size());
            println!("number of tables: {}", db.get_tables().len());
        }
        ".tables" => {
            let db = SQLiteDB::new(&args[1])?;
            for name in db.get_tables().iter().map(|table| &table.tbl_name) {
                // ref: https://www.sqlite.org/fileformat2.html#internal_schema_objects
                if !name.starts_with("sqlite_") {
                    print!("{} ", name);
                }
            }
        }
        _ => {
            let db = SQLiteDB::new(&args[1])?;
            match SQLParser::parse_stmt(command)? {
                Stmt::CreateTable(_) => unimplemented!(),
                Stmt::CreateIndex(_) => unimplemented!(),
                Stmt::Select(SelectStmt {
                    from,
                    result_column,
                    where_clause,
                }) => {
                    let schema_table = db
                        .get_table(&from)
                        .context(format!("Invalid table name {}", from))?;

                    let mut selected_columns: Vec<usize> = vec![];
                    for expr in result_column.iter() {
                        match expr {
                            Expr::Function(AggregateFunction::Count(count)) => match count {
                                Some(_) => unimplemented!(),
                                None => {
                                    let root_page = db.get_page(
                                        schema_table.rootpage.context("Table has no root page")?,
                                    )?;
                                    println!("{}", root_page.cell_count);
                                    return Ok(());
                                }
                            },
                            Expr::Column(column) => {
                                let column_def = schema_table.get_table_column_def()?;
                                match column_def.iter().position(|x| x == column) {
                                    Some(column_idx) => selected_columns.push(column_idx),
                                    None => bail!("Selected column {} doesn't exist", column),
                                }
                            }
                        }
                    }

                    let rows = db.get_table_rows(&schema_table, where_clause.as_ref())?;
                    for row in rows.iter() {
                        let mut output = vec![];
                        for column_idx in selected_columns.iter() {
                            match column_idx {
                                0 => output.push(row.row_id.to_string()),
                                _ => output.push(format!("{}", row.columns[*column_idx])),
                            }
                        }
                        println!("{}", output.join("|"));
                    }
                }
            }
        }
    }

    Ok(())
}
