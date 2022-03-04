use csv::{StringRecord, Writer};
use once_cell::sync::Lazy;
use regex::Regex;
use std::ops::Range;

static TOOLTIP_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"([-.\d]+)\s*\[([-.\d]+)\s*-([-.\d]+)]").unwrap());

pub trait RecordFilter {
    fn split_tool_tip(&self, index: Option<usize>) -> crate::Result<Vec<StringRecord>>;
    fn remove(&self, index: usize) -> crate::Result<Vec<StringRecord>>;
    fn keep_values(&self, values: &[&str], index: usize) -> crate::Result<Vec<StringRecord>>;
    fn to_csv(&self, headers: &[&str]) -> crate::Result<String>;
    fn select_ranges(&self, ranges: Vec<Range<usize>>) -> crate::Result<Vec<StringRecord>>;
    fn transpose_years(
        &self,
        base: Range<usize>,
        years: Vec<(usize, i32)>,
    ) -> crate::Result<Vec<StringRecord>>;
    fn rename_country(&self, index: usize) -> crate::Result<Vec<StringRecord>>;
}

impl RecordFilter for Vec<StringRecord> {
    fn rename_country(&self, index: usize) -> crate::Result<Vec<StringRecord>> {
        let mappings: &[(&[&str], &str)] = &[
            (&["Lao PDR", "Lao People's Democratic Republic"], "Lao PDR"),
            (
                &["Vietnam", "Viet Nam", "South Viet Nam (former)"],
                "Viet Nam",
            ),
        ];
        let mut out: Vec<_> = self
            .into_iter()
            .map(|rec| {
                let fields: Vec<_> = rec.iter().collect();
                let mut new_rec = StringRecord::new();
                for (field_index, &field_value) in fields.iter().enumerate() {
                    if index != field_index {
                        new_rec.push_field(field_value);
                    } else {
                        let mut fixed_country = field_value;
                        'fix_name: for (sources, new_name) in mappings.clone().into_iter() {
                            let field_value = field_value.trim().to_uppercase();
                            for &s in sources.iter() {
                                if s.to_uppercase() == field_value {
                                    fixed_country = new_name;
                                    break 'fix_name;
                                }
                            }
                        }
                        new_rec.push_field(fixed_country);
                    }
                }
                new_rec
            })
            .collect();
        out.sort_by(|a, b| a.get(index).unwrap().cmp(b.get(index).unwrap()));
        Ok(out)
    }

    fn split_tool_tip(&self, index: Option<usize>) -> crate::Result<Vec<StringRecord>> {
        let mut output = vec![];
        for rec in self.iter() {
            let mut new_rec = StringRecord::new();
            let fields: Vec<_> = rec.iter().collect();
            let tooltip_index = index.unwrap_or(fields.len() - 1);
            let (left, right) = fields.split_at(tooltip_index);
            for f in left {
                new_rec.push_field(f);
            }
            let caps = TOOLTIP_REGEX
                .captures(fields[tooltip_index].trim())
                .unwrap();
            new_rec.push_field(&caps[1]);
            new_rec.push_field(&caps[2]);
            new_rec.push_field(&caps[3]);
            for f in right.into_iter().skip(1) {
                new_rec.push_field(f);
            }
            output.push(new_rec);
        }
        Ok(output)
    }

    fn keep_values(&self, values: &[&str], index: usize) -> crate::Result<Vec<StringRecord>> {
        let out = self
            .into_iter()
            .filter(|r| values.into_iter().any(|v| v.trim() == r[index].trim()))
            .cloned()
            .collect();
        Ok(out)
    }

    fn remove(&self, index: usize) -> crate::Result<Vec<StringRecord>> {
        let out = self
            .into_iter()
            .map(|rec| {
                let mut fields: Vec<_> = rec.iter().collect();
                fields.remove(index);
                let mut new_rec = StringRecord::new();
                new_rec.extend(fields);
                new_rec
            })
            .collect();
        Ok(out)
    }

    fn select_ranges(&self, ranges: Vec<Range<usize>>) -> crate::Result<Vec<StringRecord>> {
        let out = self
            .into_iter()
            .map(|rec| {
                let fields: Vec<_> = rec.iter().collect();
                let mut new_rec = StringRecord::new();
                for range in ranges.iter() {
                    new_rec.extend(fields[range.clone()].iter());
                }
                new_rec
            })
            .collect();
        Ok(out)
    }

    fn to_csv(&self, headers: &[&str]) -> crate::Result<String> {
        let mut out = <Vec<u8>>::new();
        let mut writer = Writer::from_writer(&mut out);
        if headers.len() > 0 {
            writer.write_record(headers.iter())?;
        }
        for r in self.iter() {
            writer.write_record(r.iter())?;
        }
        drop(writer);
        Ok(String::from_utf8(out)?)
    }

    fn transpose_years(
        &self,
        base: Range<usize>,
        years: Vec<(usize, i32)>,
    ) -> crate::Result<Vec<StringRecord>> {
        let out = self
            .into_iter()
            .flat_map(|rec| {
                let mut new_rows = vec![];
                let fields: Vec<_> = rec.iter().collect();
                for (index, year) in years.iter().copied() {
                    let mut new_rec = StringRecord::new();
                    new_rec.extend(fields[base.clone()].iter());
                    new_rec.push_field(&year.to_string());
                    new_rec.push_field(fields[index]);
                    new_rows.push(new_rec);
                }
                new_rows
            })
            .collect();
        Ok(out)
    }
}
