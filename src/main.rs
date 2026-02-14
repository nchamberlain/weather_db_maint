use colorize::AnsiColor;
//use inquire::Select;
use sqlx::{mysql::{MySqlPoolOptions, MySqlRow}, MySql, Pool, Row};
use dotenvy::dotenv;
use std::env;


fn main()  {
    let _ = get_db_action();
}

fn get_db_action() -> Result<(), sqlx::Error>{
    let choices = vec![
        "List all cities",
        //"Add new city",
        //"Create city tables",
        "Create city sub tables",
        //"Drop city tables",
        "Drop city sub tables",
        "Insert Avgs into city sub tables",
        "Exit",
    ];

    loop {
        let prompt_message = "Please select a database action".blue();
        let select = inquire::Select::new(&prompt_message, choices.clone())
            .prompt()
            .expect("Failed to select a database action");

        //println!("You selected the action: {0}", select.green());
        if select == "List all cities" {
            println!("Listing all cities...");
            list_all_cities().expect("Failed to list all cities");
        }/* else if select == "Add new city" {
            println!("Adding a new city...UNDER CONSTRUCTION");
            // Add code to add a new city to the database
        } else if select == "Create city tables" {
            println!("Creating city tables...UNDER CONSTRUCTION");
            // Add code to create city tables in the database
        }*/ else if select == "Create city sub tables" {
            println!("Creating city sub tables...");
            create_sub_tables().expect("Failed to create city sub tables");
        } /*else if select == "Drop city tables" {
            println!("Dropping city tables...UNDER CONSTRUCTION");
            // Add code to drop city tables from the database
        }*/ else if select == "Drop city sub tables" {
            println!("Dropping city sub tables...");
            drop_sub_tables().expect("Failed to drop city sub tables");
        } else if select == "List all tables" {
            println!("Listing all tables...");
            // Add code to list all tables in the database
        } else
        if select == "Exit" {
            println!("Exiting the program. Goodbye!");
            break;
        }
    }
    Ok(())
}

#[tokio::main] 
async fn list_all_cities() -> Result<(), sqlx::Error> {
    // get the db env info from .env file
    dotenv().ok();
    // Set up the database URL from environment variable
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    // Create a connection pool
    let pool: Pool<MySql> = MySqlPoolOptions::new()
        .max_connections(5) // Set the maximum number of connections
        .connect(&database_url)
        .await?;

    let city_list_result: Result<Vec<MySqlRow>, sqlx::Error> = list_cities(&pool).await;
    match city_list_result {
        Ok(_) => { //probably only returns Ok if it found something. otherwise it would return err, no empty check
            let city_list = city_list_result.unwrap();
            for a_city in city_list {
                let c_name: &str = a_city.get("name_of_city");
                println!("Available city: {c_name}");
            }
        },
        Err(e) => eprint!("Cities not found, {} ", e),
    }   
    Ok(())
}
async fn list_cities(pool: &Pool<MySql>) -> Result<Vec<MySqlRow>, sqlx::Error> {
    let query_string = format!("SELECT name_of_city FROM city_names"); 
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_string)
        .fetch_all(pool)
        .await?; 
    Ok(rows)
}

#[tokio::main] 
async fn create_sub_tables() -> Result<(), sqlx::Error> {
    // get the db env info from .env file
    dotenv().ok();
    // Set up the database URL from environment variable
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    // Create a connection pool
    let pool: Pool<MySql> = MySqlPoolOptions::new()
        .max_connections(5) // Set the maximum number of connections
        .connect(&database_url)
        .await?;

    let cities = vec![
        "Berkeley_CA",
        "Billings_MT",
        "Bismarck_ND",
        "Chicago_IL",
        "Columbus_OH",
        "Dallas_TX",
        "Fairbanks_AK",
        "Houston_TX",
        "Indianapolis_IN",
        "Jacksonville_FL",
        "Los_Angeles_CA" ,
        "Minneapolis_MN",
        "Nashville_TN",
        "New_York_NY",
        "Oklahoma_OK",
        "Philadelphia_PA",
        "Phoenix_AZ",
        "San_Antonio_TX",
        "San_Diego_CA",
        "San_Francisco_CA",
        "Seattle_WA",
        "Spokane_WA",
    ];

    let prompt_message = "Please select the cities to CREATE sub tables".blue();
    let selected_cities = inquire::MultiSelect::new(&prompt_message, cities)
        .prompt()
        .expect("Failed to select cities");

    for the_city in selected_cities {
        println!("Creating sub tables for city of {0}", the_city.red());
        create_city_sub_tables(&pool, the_city).await?;    
    }
    Ok(())
}   

async fn create_city_sub_tables(pool: &Pool<MySql>, city: &str) -> Result<(), sqlx::Error> {
    let city_sub_month = format!("{city}_month");
    let city_sub_fort = format!("{city}_fort"); 
    let city_sub_week = format!("{city}_week"); 

  let create_month_stmt = format!(r#"CREATE TABLE if NOT exists `{}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `station` char(12) DEFAULT NULL,
  `tyear` smallint(6) NOT NULL,
  `tmonth` smallint(6) NOT NULL,
  `tmax` smallint(6) DEFAULT NULL,
  `tmin` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci ;"#, city_sub_month);
  let _result = sqlx::query(&create_month_stmt).execute(pool).await?;

  let create_week_stmt = format!(r#"CREATE TABLE if NOT exists `{}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `station` char(12) DEFAULT NULL,
  `tyear` smallint(6) NOT NULL,
  `tweek` smallint(6) NOT NULL,
  `tmax` smallint(6) DEFAULT NULL,
  `tmin` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;"#, city_sub_week);
  let _result2 = sqlx::query(&create_week_stmt).execute(pool).await?;

  let create_fort_stmt = format!(r#"CREATE TABLE if NOT EXISTS `{}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `station` char(12) DEFAULT NULL,
  `tyear` smallint(6) NOT NULL,
  `tfort` smallint(6) NOT NULL,
  `tmax` smallint(6) DEFAULT NULL,
  `tmin` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;"#, city_sub_fort);
  let _result3 = sqlx::query(&create_fort_stmt).execute(pool).await?;

    Ok(())
}

#[tokio::main] 
async fn drop_sub_tables() -> Result<(), sqlx::Error> {
    // get the db env info from .env file
    dotenv().ok();
    // Set up the database URL from environment variable
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    // Create a connection pool
    let pool: Pool<MySql> = MySqlPoolOptions::new()
        .max_connections(5) // Set the maximum number of connections
        .connect(&database_url)
        .await?;

    let cities = vec![
        "Berkeley_CA",
        "Billings_MT",
        "Bismarck_ND",
        "Chicago_IL",
        "Columbus_OH",
        "Dallas_TX",
        "Fairbanks_AK",
        "Houston_TX",
        "Indianapolis_IN",
        "Jacksonville_FL",
        "Los_Angeles_CA" ,
        "Minneapolis_MN",
        "Nashville_TN",
        "New_York_NY",
        "Oklahoma_OK",
        "Philadelphia_PA",
        "Phoenix_AZ",
        "San_Antonio_TX",
        "San_Diego_CA",
        "San_Francisco_CA",
        "Seattle_WA",
        "Spokane_WA",
    ];

    let prompt_message = "Please select the cities to DROP sub tables".blue();
    let selected_cities = inquire::MultiSelect::new(&prompt_message, cities)
        .prompt()
        .expect("Failed to select cities");

    for the_city in selected_cities {
        println!("Dropping sub tables for city of {0}", the_city.red());
        drop_city_sub_tables(&pool, the_city).await?;    
    }
    Ok(())
}   
async fn drop_city_sub_tables(pool: &Pool<MySql>, city: &str) -> Result<(), sqlx::Error>{
    let city_sub_month = format!("{city}_month");
    let city_sub_fort = format!("{city}_fort"); 
    let city_sub_week = format!("{city}_week"); 

    let drop_stmt = format!("DROP TABLE IF EXISTS {city_sub_month},{city_sub_fort},{city_sub_week};");

    let _result = sqlx::query(&drop_stmt).execute(pool).await?;

    Ok(())
}