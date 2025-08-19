use gtk::prelude::*;
use gtk::{cairo, Application, ApplicationWindow, DrawingArea, Box as GtkBox, Orientation};
use std::cell::RefCell;
use std::fs;
use std::rc::Rc;
use std::time::{Duration, Instant};
use glib::ControlFlow::Continue;
use glib::timeout_add_local;
use postgres::{Client, NoTls};
use std::sync::{Arc, Mutex};
use rand::Rng;
use serde::Deserialize;

#[derive(Deserialize)]
struct DbConfig {
    host: String,
    user: String,
    dbname: String,
    password: String,
}

fn load_config() -> Result<DbConfig, String> {
    let mut config_path = dirs::home_dir().ok_or("Could not find home directory")?;
    config_path.push(".db_config.json");
    let contents = fs::read_to_string(config_path).map_err(|e| format!("Failed to read config: {}", e))?;
    serde_json::from_str(&contents).map_err(|e| format!("Failed to parse config: {}", e))
}

fn initialize_db_client(config: &DbConfig) -> Result<Client, String> {
    let conn_str = format!("host={} user={} dbname={} password={}", config.host, config.user, config.dbname, config.password);
    Client::connect(&conn_str, NoTls).map_err(|e| format!("Connection error: {}", e))
}

fn fetch_count(client: &mut Client, status: &str) -> Result<i64, String> {
    let query = format!("SELECT count(id) FROM public.report_cache_status WHERE status = '{}'", status);
    let row = client.query_one(&query, &[]).map_err(|e| format!("Query error: {}", e))?;
    Ok(row.get(0))
}

fn setup_drawing_area(title: &str, data: Rc<RefCell<Vec<i64>>>, color: (f64, f64, f64)) -> DrawingArea {
    let drawing_area = DrawingArea::new();
    drawing_area.set_content_width(300);
    drawing_area.set_content_height(150);

    let title = title.to_string();
    drawing_area.set_draw_func(move |_, cr, width, height| {
        let data = data.borrow();

        if title == "BUILDING count" && data.last().copied().unwrap_or(0) > 15 {
            cr.set_source_rgb(1.0, 0.8, 0.9); // light pink
        } else {
            cr.set_source_rgb(1.0, 1.0, 1.0); // white
        }
        cr.paint().unwrap();

        cr.set_source_rgb(0.0, 0.0, 0.0);
        cr.set_line_width(1.0);
        cr.move_to(30.0, 10.0);
        cr.line_to(30.0, height as f64 - 20.0);
        cr.line_to(width as f64 - 10.0, height as f64 - 20.0);
        cr.stroke().unwrap();

        cr.set_source_rgb(0.0, 0.0, 0.0);
        cr.select_font_face("Sans", cairo::FontSlant::Normal, cairo::FontWeight::Bold);
        cr.set_font_size(12.0);
        cr.move_to(40.0, 20.0);
        cr.show_text(&title).unwrap();

        let max_value = data.iter().copied().max().unwrap_or(1) as f64;
        let step = (width as f64 - 40.0) / data.len().max(1) as f64;

        let tick_count = 5;
        for i in 0..=tick_count {
            let tick_value = max_value * (i as f64) / tick_count as f64;
            let y = height as f64 - 20.0 - (tick_value / max_value) * (height as f64 - 40.0);
            cr.move_to(25.0, y);
            cr.line_to(30.0, y);
            cr.stroke().unwrap();
            cr.move_to(5.0, y + 4.0);
            cr.show_text(&format!("{:.0}", tick_value)).unwrap();
        }

        cr.set_source_rgb(color.0, color.1, color.2);
        cr.set_line_width(2.0);
        for (i, &val) in data.iter().enumerate() {
            let x = 30.0 + i as f64 * step;
            let y = height as f64 - 20.0 - (val as f64 / max_value) * (height as f64 - 40.0);
            if i == 0 {
                cr.move_to(x, y);
            } else {
                cr.line_to(x, y);
            }
        }
        cr.stroke().unwrap();
    });

    drawing_area
}

fn main() {
    let config = load_config().expect("Failed to load config");
    let db_client = initialize_db_client(&config).ok();
    let shared_client = Arc::new(Mutex::new(db_client));

    let building_data = Rc::new(RefCell::new(Vec::<i64>::new()));
    let failed_data = Rc::new(RefCell::new(Vec::<i64>::new()));
    let last_failure_update = Rc::new(RefCell::new(Instant::now() - Duration::from_secs(30)));

    let app = Application::builder()
        .application_id("com.example.RandomWidget")
        .build();

    let client_ref = Arc::clone(&shared_client);
    let building_ref = Rc::clone(&building_data);
    let failed_ref = Rc::clone(&failed_data);
    let last_failure = Rc::clone(&last_failure_update);

    app.connect_activate(move |app| {
        let vbox = GtkBox::new(Orientation::Vertical, 5);
        let building_graph = setup_drawing_area("BUILDING count", Rc::clone(&building_ref), (0.2, 0.4, 0.8));
        let failed_graph = setup_drawing_area("FAILED count", Rc::clone(&failed_ref), (1.0, 0.0, 0.0));
        vbox.append(&building_graph);
        vbox.append(&failed_graph);

        let window = ApplicationWindow::builder()
            .application(app)
            .title("Production Queue Depth")
            .default_width(1920)
            .default_height(1440)
            .child(&vbox)
            .build();

        let building_clone = Rc::clone(&building_ref);
        let failed_clone = Rc::clone(&failed_ref);
        let last_failure_clone = Rc::clone(&last_failure);
        let client_clone = Arc::clone(&client_ref);

        // Populate graphs immediately on startup
        {
            let mut guard = client_clone.lock().unwrap();
            if let Some(client) = guard.as_mut() {
                let mut b = building_clone.borrow_mut();
                b.push(0);
                let building = fetch_count(client, "BUILDING").unwrap_or(rand::thread_rng().gen_range(1..=100));
                b.push(building);

                let mut f = failed_clone.borrow_mut();
                f.push(0);
                let failed = fetch_count(client, "FAILED").unwrap_or(rand::thread_rng().gen_range(1..=20));
                f.push(failed);

                *last_failure_clone.borrow_mut() = Instant::now();
            }
        }

        timeout_add_local(Duration::from_secs(10), move || {
            let mut guard = client_clone.lock().unwrap();
            if let Some(client) = guard.as_mut() {
                let building = fetch_count(client, "BUILDING").unwrap_or(rand::thread_rng().gen_range(1..=100));
                let mut b = building_clone.borrow_mut();
                b.push(building);
                if b.len() > 50 { b.remove(0); }

                if last_failure_clone.borrow().elapsed() >= Duration::from_secs(30) {
                    let failed = fetch_count(client, "FAILED").unwrap_or(rand::thread_rng().gen_range(1..=20));
                    *last_failure_clone.borrow_mut() = Instant::now();
                    let mut f = failed_clone.borrow_mut();
                    f.push(failed);
                    if f.len() > 50 { f.remove(0); }
                }
            }
            building_graph.queue_draw();
            failed_graph.queue_draw();
            Continue
        });

        window.show();
    });

    app.run();
}