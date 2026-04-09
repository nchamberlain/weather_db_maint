use colorize::AnsiColor;
//use inquire::Select;
use sqlx::{mysql::{MySqlPoolOptions, MySqlRow}, MySql, Pool, Row, FromRow};
use dotenvy::dotenv;
use std::env;
//use std::io::{self, Write};
use std::sync::OnceLock;
use std::cmp::Ordering;  //for calc of median in insert_means_and_avgs

static DB_POOL: OnceLock<Pool<MySql>> = OnceLock::new();
//fn get_db_pool() -> &'static Pool<MySql> {
//    DB_POOL.get().expect("Database pool not initialized")
//}
static DB_URL: OnceLock<String> = OnceLock::new();

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    // initialize the database pool
    dotenv().ok();
        
    DB_URL.get_or_init(|| env::var("DATABASE_URL").expect("DATABASE_URL must be set"));
    // Create a static connection pool
    DB_POOL.set(MySqlPoolOptions::new()
        .max_connections(5) // Set the maximum number of connections
        .connect(&DB_URL.get().expect("Database URL not initialized"))
        .await?)
        .expect("Failed to initialize database pool");

    let result = get_user_choice();
    Ok((result.await)?)
}

async fn get_user_choice() -> Result<(), sqlx::Error>{
    let choices = vec![
        "List all cities",
        "Create city sub tables",
        "Truncate city sub tables",
        "Drop city sub tables",
        //"Insert Avgs into city sub tables",
        "Insert Means and Avgs into city sub tables",
        "Exit",
    ];

    loop {
        let prompt_message = "Please select a database action".blue();
        let select = inquire::Select::new(&prompt_message, choices.clone())
            .prompt()
            .expect("Failed to select a database action");

        if select == "List all cities" {
            list_all_cities().await.expect("Failed to list all cities");
        } else if select == "Create city sub tables" {
            create_sub_tables().await.expect("Failed to create city sub tables");
        } else if select == "Truncate city sub tables" {
            truncate_sub_tables().await.expect("Failed to truncate city sub tables");
        } else if select == "Drop city sub tables" {
            drop_sub_tables().await.expect("Failed to drop city sub tables");
        //} else if select == "Insert Avgs into city sub tables" {
         //   insert_averages().await.expect("Failed to insert averages into city sub tables");
        } else if select == "Insert Means and Avgs into city sub tables" {
            //println!("Inserting means and averages into city sub tables...UNDER CONSTRUCTION");
            insert_medians_and_avgs().await.expect("Failed to insert means and averages into city sub tables");
        } else
        if select == "Exit" {
            println!("Exiting the program. Goodbye!");
            break;
        }
    }
    Ok(())
}
async fn truncate_sub_tables() -> Result<(), sqlx::Error> {
    let selected_cities = select_cities("Please select the cities to TRUNCATE sub tables".to_string()).await;
    for the_city in selected_cities {
        println!("Truncating sub tables for city of {0}", the_city.clone().red());
        truncate_city_sub_tables(&the_city).await?;    
    }
    Ok(())
}
async fn truncate_city_sub_tables(city: &str) -> Result<(), sqlx::Error>{
    let city_sub_month = format!("{city}_month");
    let city_sub_fort = format!("{city}_fort"); 
    let city_sub_week = format!("{city}_week"); 

    let truncate_month_stmt = format!("TRUNCATE TABLE {city_sub_month};");
    let truncate_fort_stmt = format!("TRUNCATE TABLE {city_sub_fort};");
    let truncate_week_stmt = format!("TRUNCATE TABLE {city_sub_week};");

    let _result = sqlx::query(&truncate_month_stmt)
        .execute(DB_POOL.get().expect("Database pool not initialized"))
        .await?;
    let _result = sqlx::query(&truncate_fort_stmt)
        .execute(DB_POOL.get().expect("Database pool not initialized"))
        .await?;
    let _result = sqlx::query(&truncate_week_stmt)
        .execute(DB_POOL.get().expect("Database pool not initialized"))
        .await?;

    Ok(())
}
async fn list_all_cities() -> Result<(), sqlx::Error> {
    let city_list_result: Result<Vec<MySqlRow>, sqlx::Error> = list_cities().await;
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
    println!("Listing all cities in city_names in database: {:?}" , DB_URL.get());
    Ok(())
}
async fn list_cities() -> Result<Vec<MySqlRow>, sqlx::Error> {
    let query_string = format!("SELECT name_of_city FROM city_names order by name_of_city asc;"); 
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_string)
        .fetch_all(DB_POOL.get().expect("Database pool not initialized"))
        .await?; 
    Ok(rows)
}

async fn create_sub_tables() -> Result<(), sqlx::Error> {
    let selected_cities = select_cities("Please select the cities to CREATE sub tables".to_string()).await;
    for the_city in selected_cities {
        println!("Creating sub tables for city of {0}", the_city.clone().red());
        create_city_sub_tables(&the_city).await?;    
    }
    Ok(())
}   

async fn create_city_sub_tables(city: &str) -> Result<(), sqlx::Error> {
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
  `mmax` smallint(6) DEFAULT NULL,
  `mmin` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci ;"#, city_sub_month);
  let _result = sqlx::query(&create_month_stmt)
    .execute(DB_POOL.get().expect("Database pool not initialized"))
    .await?;

  let create_week_stmt = format!(r#"CREATE TABLE if NOT exists `{}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `station` char(12) DEFAULT NULL,
  `tyear` smallint(6) NOT NULL,
  `tweek` smallint(6) NOT NULL,
  `tmax` smallint(6) DEFAULT NULL,
  `tmin` smallint(6) DEFAULT NULL,
  `mmax` smallint(6) DEFAULT NULL,
  `mmin` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;"#, city_sub_week);
  let _result2 = sqlx::query(&create_week_stmt)
    .execute(DB_POOL.get().expect("Database pool not initialized")).await?;

  let create_fort_stmt = format!(r#"CREATE TABLE if NOT EXISTS `{}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `station` char(12) DEFAULT NULL,
  `tyear` smallint(6) NOT NULL,
  `tfort` smallint(6) NOT NULL,
  `tmax` smallint(6) DEFAULT NULL,
  `tmin` smallint(6) DEFAULT NULL,
  `mmax` smallint(6) DEFAULT NULL,
  `mmin` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;"#, city_sub_fort);
  let _result3 = sqlx::query(&create_fort_stmt)
    .execute(DB_POOL.get().expect("Database pool not initialized"))
    .await?;

    Ok(())
}

async fn drop_sub_tables() -> Result<(), sqlx::Error> {
    let selected_cities = select_cities("Please select the cities to DROP sub tables".to_string()).await;
    for the_city in selected_cities {
        println!("Dropping sub tables for city of {0}", the_city.clone().red());
        drop_city_sub_tables(&the_city).await?;    
    }
    Ok(())
}   
async fn drop_city_sub_tables(city: &str) -> Result<(), sqlx::Error>{
    let city_sub_month = format!("{city}_month");
    let city_sub_fort = format!("{city}_fort"); 
    let city_sub_week = format!("{city}_week"); 

    let drop_stmt = format!("DROP TABLE IF EXISTS {city_sub_month},{city_sub_fort},{city_sub_week};");

    let _result = sqlx::query(&drop_stmt)
        .execute(DB_POOL.get().expect("Database pool not initialized"))
        .await?;

    Ok(())
}

/*async fn insert_averages() -> Result<(), sqlx::Error> {
    let selected_cities = select_cities("Please select the cities to CALC Averages for".to_string()).await;
    for the_city in selected_cities {
        println!("Calculatng sub tables for city of {0}", the_city.clone().red());

        let  first_year: i32 = get_1st_year(&the_city).await;
        let last_year: i32 = get_end_year(&the_city).await;
        println!("From {} to {}", first_year, last_year);

        let month_city = the_city.clone();
        let month_calc = tokio::spawn( async move {calc_city_month(&month_city, first_year, last_year).await});   

        let fort_city = the_city.clone();
        let fort_calc = tokio::spawn(async move {calc_city_fort(&fort_city, first_year, last_year).await});   

        let week_city = the_city.clone(); 
        let week_calc = tokio::spawn(async move {calc_city_week(&week_city, first_year, last_year).await});    

        let _week_result = week_calc.await;
        let _fort_result = fort_calc.await;
        let _month_result = month_calc.await;

    }

    Ok(())
}
*/
#[derive(Debug, FromRow, Clone)]
struct DailyTemps {
    station: String,
    tdate: Option<String>,
    tmax: Option<i32>,
    tmin: Option<i32>,
}
//the structure types are Option<T> because null values are possible in the database, 
// and FromRow will return None for those fields if they are null. 
// This allows us to handle missing data gracefully without causing a panic.
#[derive(Debug, FromRow)]
struct CalculatedTemps {
    station: String,
    tyear: i32,
    tperiod: i32,
    tmax: i32,
    tmin: i32,
    mmax: i32,
    mmin: i32,
}
async fn insert_medians_and_avgs() -> Result<(), sqlx::Error> {
    let selected_cities = select_cities("Please select the cities to CALC Means and Averages".to_string()).await;
    for the_city in selected_cities {
        println!("Calclating Medians and Averages for the city of {0}", the_city.clone().red());
        let first_year: i32 = get_1st_year(&the_city).await;
        let last_year: i32 = get_end_year(&the_city).await;
        println!("From {} to {}", first_year, last_year);

        for the_year in first_year..=last_year {
            let select_year_stmt = format!("SELECT station, tdate, tmax, tmin FROM {} WHERE tdate LIKE '{}-%';", the_city, the_year);
            let dtemps: Vec<DailyTemps> = sqlx::query_as(&select_year_stmt) //build a vector of DailyTemps structs for the year
                .fetch_all(DB_POOL.get().expect("Database pool not initialized"))
                .await?;
            let month_city = the_city.clone();
            let month_temps = dtemps.clone();
            let month_calc = tokio::spawn(async move {insert_monthly_medians_and_avgs(&month_temps, the_year, &month_city).await});

            let fort_city = the_city.clone();
            let fort_temps = dtemps.clone();
            let fort_calc = tokio::spawn(async move {insert_fortly_medians_and_avgs(&fort_temps, the_year, &fort_city).await});

            let week_city = the_city.clone();
            let week_temps = dtemps.clone();
            let week_calc = tokio::spawn(async move {insert_weekly_medians_and_avgs(&week_temps, the_year, &week_city).await});

            let _month_result = month_calc.await;
            let _fort_result = fort_calc.await;
            let _week_result = week_calc.await;
        }
    } // end of the_city in selected_cities 
    Ok(()) 
}
async fn insert_monthly_medians_and_avgs(daily_temps: &Vec<DailyTemps>, the_year: i32, the_city: &str) -> Result<(), sqlx::Error> {
    let mut cmtemps: Vec<CalculatedTemps> = Vec::new(); //prepare a vector to hold the calculated monthly temps for the year
    for the_month in 1..=12 {
        let mut highs: Vec<i32> = Vec::new();
        let mut lows: Vec<i32> = Vec::new();
        let mtemps: Vec<&DailyTemps> = daily_temps.iter()
            .filter(|&month| month.tdate.as_ref().unwrap()[5..7] == format!("{:02}", the_month))
            .clone()
            .collect();
        if mtemps.len() > 0 {
            let mhigh: i32 = mtemps.iter()
                .filter_map(|&temp| {
                    if let Some(tmax) = temp.tmax {
                        highs.push(tmax);
                        Some(tmax)
                    } else {
                        None
                    }
                })
                .sum();
            let mlow: i32 = mtemps.iter()
                .filter_map(|&temp| {
                    if let Some(tmin) = temp.tmin {
                        lows.push(tmin);
                        Some(tmin)
                    } else {
                        None
                    }
                })
                .sum();   
            let mut mhigh_median: f32 = 333.0;
            let mut mlow_median: f32 = 444.0;
            if highs.len() > 0 {
                mhigh_median = median(&highs).unwrap();
            }
            if lows.len() > 0 {
                mlow_median = median(&lows).unwrap();
            }
            cmtemps.push(CalculatedTemps {
                station: mtemps[0].station.clone(),
                tyear: the_year,
                tperiod: the_month,
                tmax: (mhigh as f32 / mtemps.len() as f32).round() as i32,
                tmin: (mlow as f32 / mtemps.len() as f32).round() as i32,
                mmax: mhigh_median as i32,
                mmin: mlow_median as i32,
            });
        } else {
            cmtemps.push(CalculatedTemps {
                station: "None".to_string(),
                tyear: the_year,
                tperiod: the_month,
                tmax: 333,
                tmin: 222,
                mmax: 555,
                mmin: 444,
            });
        }
    } // end of the_month loop
    let mut insert_string = " ".to_string();
    for c in cmtemps {
        insert_string.push_str(format!("('{}',{},{},{},{},{},{}),", c.station, c.tyear, c.tperiod, c.tmax, c.tmin, c.mmax, c.mmin).as_str());
    }
    //create a bulk insert statement
    let insert_stmt = format!("INSERT INTO {}_month (station, tyear, tmonth, tmax, tmin, mmax, mmin) VALUES {};", the_city, insert_string.trim_end_matches(','));
    let _result = sqlx::query(&insert_stmt)
        .execute(DB_POOL.get().expect("Database pool not initialized"))
        .await;    
    Ok(())
}
async fn insert_fortly_medians_and_avgs(daily_temps: &Vec<DailyTemps>, the_year: i32, the_city: &str) -> Result<(), sqlx::Error> {
    let mut cftemps: Vec<CalculatedTemps> = Vec::new(); //prepare a vector to hold the calculated monthly temps for the year

    let mut low_fort = String::from("01-01");

    for the_fort in 1..=26 {
        let mut highs: Vec<i32> = Vec::new();
        let mut lows: Vec<i32> = Vec::new();

        let high_fort = get_next_fort(the_fort-1);
        let low_date= format!("{}-{}", the_year, low_fort);
        let high_date= format!("{}-{}", the_year, high_fort);

        let mtemps: Vec<&DailyTemps> = daily_temps.iter()
            .filter(|&fort| fort.tdate.as_ref().unwrap() >= &low_date && fort.tdate.as_ref().unwrap() < &high_date)
            .clone()
            .collect();
        if mtemps.len() > 0 {
            let mhigh: i32 = mtemps.iter()
                .filter_map(|&temp| {
                    if let Some(tmax) = temp.tmax {
                        highs.push(tmax);
                        Some(tmax)
                    } else {
                        None
                    }
                })
                .sum();
            let mlow: i32 = mtemps.iter()
                .filter_map(|&temp| {
                    if let Some(tmin) = temp.tmin {
                        lows.push(tmin);
                        Some(tmin)
                    } else {
                        None
                    }
                })
                .sum();   
            let mut mhigh_median: f32 = 555.0;
            let mut mlow_median: f32 = 444.0;
            if highs.len() > 0 {
                mhigh_median = median(&highs).unwrap();
            }
            if lows.len() > 0 {
                mlow_median = median(&lows).unwrap();
            }
            cftemps.push(CalculatedTemps {
                station: mtemps[0].station.clone(),
                tyear: the_year,
                tperiod: the_fort,
                tmax: (mhigh as f32 / mtemps.len() as f32).round() as i32,
                tmin: (mlow as f32 / mtemps.len() as f32).round() as i32,
                mmax: mhigh_median as i32,
                mmin: mlow_median as i32,
            });
        } else {
            cftemps.push(CalculatedTemps {
                station: "None".to_string(),
                tyear: the_year,
                tperiod: the_fort,
                tmax: 333,
                tmin: 222,
                mmax: 555,
                mmin: 444,
            });
        }
        low_fort = high_fort; //update low fort for next loop
    } // end of the_month loop
    let mut insert_string = " ".to_string();
    for c in cftemps {
        insert_string.push_str(format!("('{}',{},{},{},{},{},{}),", c.station, c.tyear, c.tperiod, c.tmax, c.tmin, c.mmax, c.mmin).as_str());
    }
    //create a bulk insert statement
    let insert_stmt = format!("INSERT INTO {}_fort (station, tyear, tfort, tmax, tmin, mmax, mmin) VALUES {};", the_city, insert_string.trim_end_matches(','));
    let _result = sqlx::query(&insert_stmt)
        .execute(DB_POOL.get().expect("Database pool not initialized"))
        .await;    
    Ok(())
}
async fn insert_weekly_medians_and_avgs(daily_temps: &Vec<DailyTemps>, the_year: i32, the_city: &str) -> Result<(), sqlx::Error> {
    let mut cwtemps: Vec<CalculatedTemps> = Vec::new(); //prepare a vector to hold the calculated weekly temps for the year

    let mut low_week = String::from("01-01");

    for the_week in 1..=52 {
        let mut highs: Vec<i32> = Vec::new();
        let mut lows: Vec<i32> = Vec::new();

        let high_week = get_next_week(the_week-1);
        let low_date= format!("{}-{}", the_year, low_week);
        let high_date= format!("{}-{}", the_year, high_week);

        let mtemps: Vec<&DailyTemps> = daily_temps.iter()
            .filter(|&week| week.tdate.as_ref().unwrap() >= &low_date && week.tdate.as_ref().unwrap() < &high_date)
            .clone()
            .collect();
        if mtemps.len() > 0 {
            let mhigh: i32 = mtemps.iter()
                .filter_map(|&temp| {
                    if let Some(tmax) = temp.tmax {
                        highs.push(tmax);
                        Some(tmax)
                    } else {
                        None
                    }
                })
                .sum();
            let mlow: i32 = mtemps.iter()
                .filter_map(|&temp| {
                    if let Some(tmin) = temp.tmin {
                        lows.push(tmin);
                        Some(tmin)
                    } else {
                        None
                    }
                })
                .sum();   
            let mut mhigh_median: f32 = 333.0;
            let mut mlow_median: f32 = 444.0;
            if highs.len() > 0 {
                mhigh_median = median(&highs).unwrap();
            }
            if lows.len() > 0 {
                mlow_median = median(&lows).unwrap();
            }
            cwtemps.push(CalculatedTemps {
                station: mtemps[0].station.clone(),
                tyear: the_year,
                tperiod: the_week,
                tmax: (mhigh as f32 / mtemps.len() as f32).round() as i32,
                tmin: (mlow as f32 / mtemps.len() as f32).round() as i32,
                mmax: mhigh_median as i32,
                mmin: mlow_median as i32,
            });
        } else {
            cwtemps.push(CalculatedTemps {
                station: "None".to_string(),
                tyear: the_year,
                tperiod: the_week,
                tmax: 333,
                tmin: 222,
                mmax: 555,
                mmin: 444,
            });
        }
        low_week = high_week;
    } // end of the_week loop, this year's weekly results collected into cwtemps vector
    let mut insert_string = " ".to_string();
    for c in cwtemps {
        insert_string.push_str(format!("('{}',{},{},{},{},{},{}),", c.station, c.tyear, c.tperiod, c.tmax, c.tmin, c.mmax, c.mmin).as_str());
    }
    //create a bulk insert statement
    let insert_stmt = format!("INSERT INTO {}_week (station, tyear, tweek, tmax, tmin, mmax, mmin) VALUES {};", the_city, insert_string.trim_end_matches(','));
    let _result = sqlx::query(&insert_stmt)
        .execute(DB_POOL.get().expect("Database pool not initialized"))
        .await;    
    Ok(())
}
/*async fn calc_city_month(city: &str, first_year: i32, last_year: i32) -> Result<(), sqlx::Error> {
    let city_sub_month = format!("{city}_month");
    println!("Starting monthly calcs for {first_year} thru {last_year}");
    io::stdout().flush().unwrap(); // force flush now

    for the_year in first_year..=last_year {
        for the_month in 1..=12 {
            let insert_month = format!("INSERT INTO {city_sub_month} (station, tyear, tmonth, tmax, tmin) VALUES 
((SELECT station from {city} WHERE tdate LIKE '{the_year}-{the_month:02}%' LIMIT 1),
 {the_year}, 
 {the_month}, 
 round((select avg(tmax) from {city} WHERE tdate LIKE '{the_year}-{the_month:02}%')), 
 round((select avg(tmin) from {city} WHERE tdate LIKE '{the_year}-{the_month:02}%')) );");
            let _result = sqlx::query(&insert_month)
                .execute(DB_POOL.get().expect("Database pool not initialized"))
                .await?;
        }
    }
    println!("Finished monthly calcs for {first_year} thru {last_year}");
    io::stdout().flush().unwrap(); // force flush now
    Ok(())
}
//WARNING: this fn doesn't calc correctly because BETWEEN low_date and high_date is INCLUSIVE so high_date is included.
async fn calc_city_fort(city: &str, first_year: i32, last_year: i32) -> Result<(), sqlx::Error> {
    let city_sub_fort = format!("{city}_fort"); 
    println!("Starting fortnightly calcs for {first_year} thru {last_year}");
    io::stdout().flush().unwrap(); // force flush now

    for the_year in first_year..=last_year {
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
            let _result = sqlx::query(&insert_fort)
                .execute(DB_POOL.get().expect("Database pool not initialized"))
                .await?;
            low_fort = high_fort; //update low fort for next loop
        }
    }
    println!("Finished fortnight calcs for {first_year} thru {last_year}");
    io::stdout().flush().unwrap(); // force flush now
    Ok(())
}
//WARNING: this fn doesn't calc correctly because BETWEEN low_date and high_date is INCLUSIVE so high_date is include.
async fn calc_city_week(city: &str, first_year: i32, last_year: i32) -> Result<(), sqlx::Error> {
    let city_sub_week = format!("{city}_week"); 
    println!("Starting weekly calcs for {first_year} thru {last_year}");
    io::stdout().flush().unwrap(); // force flush now

    for the_year in first_year..=last_year {
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
            let _result = sqlx::query(&insert_week)
                .execute(DB_POOL.get().expect("Database pool not initialized"))
                .await?;
            low_week = high_week;            
        }
    }
    println!("Starting weekly calcs for {first_year} thru {last_year}");
    io::stdout().flush().unwrap(); // force flush now
    Ok(())
}
*/
/*async fn get_first_year(city: &str) -> Result<Vec<MySqlRow>, sqlx::Error> {
    let query_stmt_string = format!("SELECT tdate FROM {city} order by tdate asc limit 1");
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_stmt_string)
        .fetch_all(DB_POOL.get().expect("Database pool not initialized"))
        .await?; 
    Ok(rows)
}*/
async fn get_1st_year(city: &str) -> i32{
    let query_stmt_string = format!("SELECT tdate FROM {city} order by tdate asc limit 1");
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_stmt_string)
        .fetch_all(DB_POOL.get().expect("Database pool not initialized"))
        .await.expect("Failed to fetch first year");
    match rows.len() {
         0 => { eprintln!("No data found for city: {}", city); return 0; },
         _ => {
            let first_year_row = &rows[0]; //unwrap the row
            let first_year_str: &str = first_year_row.get("tdate"); //get date string, for ex. 2020-09-05
            let first_year = first_year_str[0..4].parse().unwrap();  //parse first 4 digits as an int
            return first_year;
        }
    }
}
/*async fn get_last_year(city: &str) -> Result<Vec<MySqlRow>, sqlx::Error> {
    let query_stmt_string = format!("SELECT tdate FROM {city} order by tdate desc limit 1");
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_stmt_string)
        .fetch_all(DB_POOL.get().expect("Database pool not initialized"))
        .await?; // had to make this function return a Result to use the ? operator
    Ok(rows)
}*/
async fn get_end_year(city: &str) -> i32 {
    let query_stmt_string = format!("SELECT tdate FROM {city} order by tdate desc limit 1");
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_stmt_string)
        .fetch_all(DB_POOL.get().expect("Database pool not initialized"))
        .await.expect("Failed to fetch last year");
    match rows.len() {
         0 => { eprintln!("No data found for city: {}", city); return 0; },
         _ => {
            let last_year_row = &rows[0]; //unwrap the row
            let last_year_str: &str = last_year_row.get("tdate"); //get date string, for ex. 2020-11-21
            let last_year = last_year_str[0..4].parse().unwrap();  //parse first 4 digits as an int
            return last_year;
        }
    }
}

const FORTS:[&str; 26] = ["01-15", "01-29", "02-12", "02-26", "03-12", "03-26", "04-09", "04-23", "05-07", "05-21", "06-04", "06-18", "07-02", "07-16", "07-30", "08-13", "08-27", "09-10", "09-24", "10-08", "10-22", "11-05", "11-19", "12-03", "12-17", "12-32"];

fn get_next_fort(idx: i32) -> String {
    FORTS[(idx) as usize].to_string()   
}

const WEEKS:[&str; 52] = ["01-08", "01-15", "01-22", "01-29", "02-05", "02-12", "02-19", "02-26", "03-05", "03-12", "03-19", "03-26", "04-02", "04-09", "04-16", "04-23", "04-30", "05-07", "05-14", "05-21", "05-28", "06-04", "06-11", "06-18", "06-25", "07-02", "07-09", "07-16", "07-23", "07-30", "08-06", "08-13", "08-20", "08-27", "09-03", "09-10", "09-17", "09-24", "10-01", "10-08", "10-15", "10-22", "10-29", "11-05", "11-12", "11-19", "11-26", "12-03", "12-10", "12-17", "12-24", "12-32"];

fn get_next_week(idx: i32) -> String {
    WEEKS[(idx) as usize].to_string()   
}
async fn select_cities(message: String) -> Vec<String> {
    let city_list_result: Result<Vec<MySqlRow>, sqlx::Error> = list_cities().await;
    let mut cities: Vec<String> = Vec::new();
    
    match city_list_result {
        Ok(_) => { //probably only returns Ok if it found something. otherwise it would return err, no empty check
            let city_list = city_list_result.unwrap();
            for a_city in city_list {
                let c_name: String = a_city.get("name_of_city");
                cities.push(c_name);
            }
        },
        Err(e) => eprint!("Cities not found, {} ", e),
    }   

    let prompt_message = message.green();
    let selected_cities = inquire::MultiSelect::new(&prompt_message, cities)
        .prompt()
        .expect("Failed to select cities");
    return selected_cities
}
// this code from https://rust-lang-nursery.github.io/rust-cookbook/science/mathematics/statistics.html
fn partition(data: &[i32]) -> Option<(Vec<i32>, i32, Vec<i32>)> {
    match data.len() {
        0 => None,
        _ => {
            let (pivot_slice, tail) = data.split_at(1);
            let pivot = pivot_slice[0];
            let (left, right) = tail.iter()
                .fold((vec![], vec![]), |mut splits, next| {
                    {
                        //let (ref mut left, ref mut right) = &mut splits;
                        let (left, right) = &mut splits;
                        if next < &pivot {
                            left.push(*next);
                        } else {
                            right.push(*next);
                        }
                    }
                    splits
                });

            Some((left, pivot, right))
        }
    }
}

fn select(data: &[i32], k: usize) -> Option<i32> {
    let part = partition(data);

    match part {
        None => None,
        Some((left, pivot, right)) => {
            let pivot_idx = left.len();

            match pivot_idx.cmp(&k) {
                Ordering::Equal => Some(pivot),
                Ordering::Greater => select(&left, k),
                Ordering::Less => select(&right, k - (pivot_idx + 1)),
            }
        },
    }
}

fn median(data: &[i32]) -> Option<f32> {
    let size = data.len();
    match size {
        even if even % 2 == 0 => {
            let fst_med = select(data, (even / 2) - 1);
            let snd_med = select(data, even / 2);
            //println!("\nfst {} snd {}", fst_med.unwrap(), snd_med.unwrap());
            match (fst_med, snd_med) {
                (Some(fst), Some(snd)) => Some((fst + snd) as f32 / 2.0),
                _ => None
            }
        },
        odd => select(data, odd / 2).map(|x| x as f32)
    }
}
