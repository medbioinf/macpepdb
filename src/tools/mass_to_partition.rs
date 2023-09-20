use std::thread::AccessError;

use anyhow::Result;
use tracing::debug;

pub fn mass_to_partition_index(partition_limits: &Vec<i64>, mass: i64) -> Option<i64> {
    let mut lower = 0;
    let mut upper = partition_limits.len() - 1;

    while lower != upper {
        let mid = (upper + lower) / 2;

        if mass <= partition_limits[mid] && (mid == 0 || mass > partition_limits[mid - 1]) {
            return Some(mid as i64);
        }

        if mass <= partition_limits[mid] {
            upper = mid - 1;
        } else {
            lower = mid + 1;
        }
    }

    if mass <= partition_limits[lower] && (lower == 0 || mass > partition_limits[lower - 1]) {
        return Some(lower as i64);
    }

    None
}

#[cfg(test)]
mod test {
    use tracing_test::traced_test;

    use crate::tools::mass_to_partition::mass_to_partition_index;

    #[traced_test]
    #[test]
    fn test_mass_to_partition() {
        let partition_limits = vec![20, 35, 50];

        let actual_partition = mass_to_partition_index(&partition_limits, 30).unwrap();
        assert_eq!(actual_partition, 1);

        let actual_partition = mass_to_partition_index(&partition_limits, 0).unwrap();
        assert_eq!(actual_partition, 0);

        let actual_partition = mass_to_partition_index(&partition_limits, 20).unwrap();
        assert_eq!(actual_partition, 0);

        let actual_partition = mass_to_partition_index(&partition_limits, 51);
        assert_eq!(actual_partition, None);

        let actual_partition = mass_to_partition_index(&partition_limits, 35).unwrap();
        assert_eq!(actual_partition, 1);

        let actual_partition = mass_to_partition_index(&partition_limits, 50).unwrap();
        assert_eq!(actual_partition, 2);
    }
}
