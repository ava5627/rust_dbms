use owo_colors::{AnsiColors, OwoColorize};

pub fn rainbow(content: &str, num: usize) -> String {
    let colors = ["magenta", "red", "cyan", "green", "blue", "white"];
    let color: AnsiColors = colors[num % colors.len()].into();
    return content.color(color).to_string();
}
