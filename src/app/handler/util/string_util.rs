use regex::Regex;

const REGEXP_NUM: &str = r"^[0-9]*$";
const REGEXP_NAME: &str = r"^[a-zA-Z][a-zA-Z0-9_]*$";
const REGEXP_BIG_NAME: &str = r"^[A-Z][a-zA-Z0-9_]*$";
const REGEXP_SMALL_NAME: &str = r"^[a-z][a-zA-Z0-9_]*$";

pub fn is_num(s: &str) -> bool {
    let re = Regex::new(REGEXP_NUM).unwrap();
    return re.is_match(s)
}

pub fn is_name(s: &str) -> bool {
    let re = Regex::new(REGEXP_NAME).unwrap();
    return re.is_match(s)
}

pub fn is_big_name(s: &str) -> bool {
    let re = Regex::new(REGEXP_BIG_NAME).unwrap();
    return re.is_match(s)
}

pub fn is_small_name(s: &str) -> bool {
    let re = Regex::new(REGEXP_SMALL_NAME).unwrap();
    return re.is_match(s)
}