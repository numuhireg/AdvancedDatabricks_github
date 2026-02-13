valid_pages = {"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"}
 # when there in ivalid data give un warning but append the record
@dp.table
@dp.expect_all(valid_pages)
def raw_data():
  # Create a raw dataset

## if there is error in the record , drop those record
@dp.table
@dp.expect_all_or_drop(valid_pages)
def prepared_data():
  # Create a cleaned and prepared dataset

## if only on is not respect the rules drop whole dataset
@dp.table
@dp.expect_all_or_fail(valid_pages)
def customer_facing_data():
  # Create cleaned and prepared to share the dataset