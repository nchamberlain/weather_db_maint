use colorize::AnsiColor;
//use inquire::Select;
use sqlx::{mysql::{MySqlPoolOptions, MySqlRow}, MySql, Pool, Row};
use dotenvy::dotenv;
use std::env;
use std::io::{self, Write};

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
        } else if select == "Insert Avgs into city sub tables" {
            println!("Inserting averages into city sub tables...");
            insert_averages().expect("Failed to insert averages into city sub tables");
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
#[tokio::main] 
async fn insert_averages() -> Result<(), sqlx::Error> {
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

    let prompt_message = "Please select the cities to calculate averages for".green();
    let selected_cities = inquire::MultiSelect::new(&prompt_message, cities)
        .prompt()
        .expect("Failed to select cities");

    for the_city in selected_cities {
        println!("Calculatng sub tables for city of {0}", the_city.red());

        let mut first_year: i32 = 0;
        let mut last_year: i32 = 0;

        let first_year_result: Result<Vec<sqlx::mysql::MySqlRow>, sqlx::Error> = get_first_year(&pool, the_city).await;
        match first_year_result {
            Ok(_) => { 
                let first_year_row = &first_year_result.unwrap(); //unwrap the row
                let first_year_str: &str = first_year_row[0].get("tdate"); //get date string, for ex. 2020-09-05
                first_year = first_year_str[0..4].parse().unwrap();  //parse first 4 digits as an int
                println!("First year for {}: {}", the_city, first_year);
            },
            Err(e) => eprintln!("Error executing function: {}", e),
        } 

        let last_year_result: Result<Vec<sqlx::mysql::MySqlRow>, sqlx::Error> = get_last_year(&pool, the_city).await;
        match last_year_result {
            Ok(_) => { 
                let last_year_row = &last_year_result.unwrap(); //unwrap the row
                let last_year_str: &str = last_year_row[0].get("tdate"); //get date string, for ex. 2020-11-21
                last_year = last_year_str[0..4].parse().unwrap();  //parse first 4 digits as an int
                println!("Last year for {}: {}", the_city, last_year);
            },
            Err(e) => eprintln!("Error executing function: {}", e),
        } 
        calc_city_month(&pool, the_city, first_year, last_year).await?;    
        calc_city_fort(&pool, the_city, first_year, last_year).await?;    
        calc_city_week(&pool, the_city, first_year, last_year).await?;    
    }

    Ok(())
}

async fn calc_city_month(pool: &Pool<MySql>, city: &str, first_year: i32, last_year: i32) -> Result<(), sqlx::Error> {
    let city_sub_month = format!("{city}_month");

    for the_year in first_year..=last_year {
        print!("{the_year},");
        io::stdout().flush().unwrap(); // force flush now

        for the_month in 1..=12 {
            let insert_month = format!("INSERT INTO {city_sub_month} (station, tyear, tmonth, tmax, tmin) VALUES 
((SELECT station from {city} WHERE tdate LIKE '{the_year}-{the_month:02}%' LIMIT 1),
 {the_year}, 
 {the_month}, 
 round((select avg(tmax) from {city} WHERE tdate LIKE '{the_year}-{the_month:02}%')), 
 round((select avg(tmin) from {city} WHERE tdate LIKE '{the_year}-{the_month:02}%')) );");
            let _result = sqlx::query(&insert_month).execute(pool).await?;
        }
    }
    Ok(())
}
async fn calc_city_fort(pool: &Pool<MySql>, city: &str, first_year: i32, last_year: i32) -> Result<(), sqlx::Error> {
    let city_sub_fort = format!("{city}_fort"); 

    for the_year in first_year..=last_year {
        print!("{the_year}|");
        io::stdout().flush().unwrap(); // force flush now

        let mut low_fort = String::from("01-01");
        for the_fort in 1..=26 {
            let high_fort = get_next_fort(the_fort-1);
            let low_date = format!("{}-{}", the_year, low_fort);
            let high_date = format!("{}-{}", the_year, high_fort);


            let insert_fort = format!("INSERT INTO {city_sub_fort} (station, tyear, tfort, tmax, tmin) VALUES 
((SELECT station from {city} WHERE tdate BETWEEN '{low_date}' AND '{high_date}' LIMIT 1),
 {the_year}, 
 {the_fort}, 
 round((select avg(tmax) from {city} WHERE tdate BETWEEN '{low_date}' AND '{high_date}')), 
 round((select avg(tmin) from {city} WHERE tdate BETWEEN '{low_date}' AND '{high_date}')) );");
            let _result = sqlx::query(&insert_fort).execute(pool).await?;
            low_fort = high_fort; //update low fort for next loop
        }
    }
    Ok(())
}
async fn calc_city_week(pool: &Pool<MySql>, city: &str, first_year: i32, last_year: i32) -> Result<(), sqlx::Error> {
    let city_sub_week = format!("{city}_week"); 

    for the_year in first_year..=last_year {
        print!("{the_year}-");
        io::stdout().flush().unwrap(); // force flush now

        let mut low_week = String::from("01-01");
        for the_week in 1..=52 {
            let high_week = get_next_week(the_week-1);
            let low_date= format!("{}-{}", the_year, low_week);
            let high_date= format!("{}-{}", the_year, high_week);
        
           let insert_week = format!("INSERT INTO {city_sub_week} (station, tyear, tweek, tmax, tmin) VALUES 
((SELECT station from {city} WHERE tdate BETWEEN '{low_date}' AND '{high_date}' LIMIT 1),
 {the_year}, 
 {the_week}, 
 round((select avg(tmax) from {city} WHERE tdate BETWEEN '{low_date}' AND '{high_date}')), 
 round((select avg(tmin) from {city} WHERE tdate BETWEEN '{low_date}' AND '{high_date}')) );");
            let _result = sqlx::query(&insert_week).execute(pool).await?;
            low_week = high_week;            
        }
    }
    Ok(())
}

async fn get_first_year(pool: &Pool<MySql>,  city: &str) -> Result<Vec<MySqlRow>, sqlx::Error> {
    let query_stmt_string = format!("SELECT tdate FROM {city} order by tdate asc limit 1");
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_stmt_string)
        .fetch_all(pool)
        .await?; 
    //println!("Number of First Year Rows found: {}", rows.len());
    Ok(rows)
}

async fn get_last_year(pool: &Pool<MySql>,  city: &str) -> Result<Vec<MySqlRow>, sqlx::Error> {
    let query_stmt_string = format!("SELECT tdate FROM {city} order by tdate desc limit 1");
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_stmt_string)
        .fetch_all(pool)
        .await?; // had to make this function return a Result to use the ? operator
    //println!("Number of Last Year Rows found: {}", rows.len());
    Ok(rows)
}

const FORTS:[&str; 26] = ["01-15", "01-29", "02-12", "02-26", "03-12", "03-26", "04-09", "04-23", "05-07", "05-21", "06-04", "06-18", "07-02", "07-16", "07-30", "08-13", "08-27", "09-10", "09-24", "10-08", "10-22", "11-05", "11-19", "12-03", "12-17", "12-32"];

fn get_next_fort(idx: i32) -> String {
    FORTS[(idx) as usize].to_string()   
}

const WEEKS:[&str; 52] = ["01-08", "01-15", "01-22", "01-29", "02-05", "02-12", "02-19", "02-26", "03-05", "03-12", "03-19", "03-26", "04-02", "04-09", "04-16", "04-23", "04-30", "05-07", "05-14", "05-21", "05-28", "06-04", "06-11", "06-18", "06-25", "07-02", "07-09", "07-16", "07-23", "07-30", "08-06", "08-13", "08-20", "08-27", "09-03", "09-10", "09-17", "09-24", "10-01", "10-08", "10-15", "10-22", "10-29", "11-05", "11-12", "11-19", "11-26", "12-03", "12-10", "12-17", "12-24", "12-32"];

fn get_next_week(idx: i32) -> String {
    WEEKS[(idx) as usize].to_string()   
}
