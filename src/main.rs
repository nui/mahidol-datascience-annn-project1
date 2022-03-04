#![allow(dead_code)]

use csv::{Reader, StringRecord};

use crate::filters::RecordFilter;

mod filters;

type Result<T> = anyhow::Result<T>;

const AIR_POLLUTION_HEADERS: &[&str] = &[
    "Location",
    "Dim2",
    "Indicator",
    "Year",
    "Dim1",
    "Value",
    "Lowest",
    "Highest",
];

const CLEAN_FUEL_HEADERS: &[&str] = &["Location", "Indicator", "Year", "Value"];

const INFANT_MORTALITY_HEADERS: &[&str] = &[
    "Location",
    "Year",
    "Indicator",
    "Dim1",
    "Value",
    "Lowest",
    "Highest",
];

const MATERNAL_MORTALITY_RATIO_HEADERS: &[&str] = &[
    "Location",
    "Year",
    "Indicator",
    "Value",
    "Lowest",
    "Highest",
];

const NEONATAL_MORTALITY_RATE_HEADERS: &[&str] = &[
    "Location",
    "Year",
    "Indicator",
    "Dim1",
    "Value",
    "Lowest",
    "Highest",
];

const SEA_COUNTRIES: &[&str] = &[
    "Brunei Darussalam",
    "Cambodia",
    "Indonesia",
    "Lao PDR",
    "Malaysia",
    "Myanmar",
    "Philippines",
    "Singapore",
    "Thailand",
    "Timor-Leste",
    "Viet Nam",
];

const PM25_HEADERS: &[&str] = &[
    "Location",
    "LocationCode",
    "IndicatorName",
    "IndicatorCode",
    "Year",
    "Value",
];

const CO2_HEADERS: &[&str] = &[
    "Location",
    "LocationCode",
    "IndicatorName",
    "IndicatorCode",
    "Year",
    "Value",
];

fn main() -> crate::Result<()> {
    let rendered = extract_records("data/airPollutionDeathRate.csv")?
        .rename_country(0)?
        .split_tool_tip(None)?
        .keep_values(&SEA_COUNTRIES, 0)?
        .to_csv(AIR_POLLUTION_HEADERS)?;
    std::fs::write("output/airPollutionDeathRate.csv", rendered)?;

    let rendered = extract_records("data/cleanFuelAndTech.csv")?
        .rename_country(0)?
        .keep_values(&SEA_COUNTRIES, 0)?
        .to_csv(CLEAN_FUEL_HEADERS)?;
    std::fs::write("output/cleanFuelAndTech.csv", rendered)?;

    let rendered = extract_records("data/infantMortalityRate.csv")?
        .rename_country(0)?
        .split_tool_tip(None)?
        .keep_values(&SEA_COUNTRIES, 0)?
        .to_csv(INFANT_MORTALITY_HEADERS)?;
    std::fs::write("output/infantMortalityRate.csv", rendered)?;

    let rendered = extract_records("data/maternalMortalityRatio.csv")?
        .rename_country(0)?
        .split_tool_tip(None)?
        .keep_values(&SEA_COUNTRIES, 0)?
        .to_csv(MATERNAL_MORTALITY_RATIO_HEADERS)?;
    std::fs::write("output/maternalMortalityRatio.csv", rendered)?;

    let rendered = extract_records("data/neonatalMortalityRate.csv")?
        .rename_country(0)?
        .split_tool_tip(None)?
        .keep_values(&SEA_COUNTRIES, 0)?
        .to_csv(NEONATAL_MORTALITY_RATE_HEADERS)?;
    std::fs::write("output/neonatalMortalityRate.csv", rendered)?;

    let years: Vec<(usize, i32)> = (4..).zip(2010..=2017).collect();
    let rendered = extract_records("data/API_EN.ATM.PM25.MC.M3_DS2_en_csv_v2_3681254.csv")?
        .rename_country(0)?
        .keep_values(&SEA_COUNTRIES, 0)?
        .select_ranges(vec![0..4, 54..62])?
        .transpose_years(0..4, years.clone())?
        .to_csv(PM25_HEADERS)?;
    std::fs::write("output/PM25.csv", rendered)?;

    let rendered = extract_records("data/API_EN.ATM.CO2E.PC_DS2_en_csv_v2_3638608.csv")?
        .rename_country(0)?
        .keep_values(&SEA_COUNTRIES, 0)?
        .select_ranges(vec![0..4, 54..62])?
        .transpose_years(0..4, years.clone())?
        .to_csv(CO2_HEADERS)?;
    std::fs::write("output/co2.csv", rendered)?;

    let rendered = extract_records("data/API_EN.ATM.GHGT.KT.CE_DS2_en_csv_v2_3653969.csv")?
        .rename_country(0)?
        .keep_values(&SEA_COUNTRIES, 0)?
        .select_ranges(vec![0..4, 54..62])?
        .transpose_years(0..4, years.clone())?
        .to_csv(CO2_HEADERS)?;
    std::fs::write("output/greenhouse.csv", rendered)?;

    let rendered = extract_records("data/API_AG.LND.FRST.ZS_DS2_en_csv_v2_3630712.csv")?
        .rename_country(0)?
        .keep_values(&SEA_COUNTRIES, 0)?
        .select_ranges(vec![0..4, 54..62])?
        .transpose_years(0..4, years.clone())?
        .to_csv(CO2_HEADERS)?;
    std::fs::write("output/forest_area.csv", rendered)?;
    Ok(())
}

fn extract_records(path: &str) -> crate::Result<Vec<StringRecord>> {
    Ok(Reader::from_path(path)?.records().flatten().collect())
}
