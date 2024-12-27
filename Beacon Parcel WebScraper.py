from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import undetected_chromedriver as uc
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import csv
from datetime import datetime
import os

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--disable-popup-blocking")
chrome_options.add_argument("--disable-notifications")

# Initialize the Chrome driver with options
driver = uc.Chrome(options=chrome_options)

# Class codes for properties
class_codes = {
    "401": "4 - 19 family apartments",
    "402": "20 - 39 family apartments",
    "403": "40+ family apartments",
    "530": "Three Family Dwelling Platted",
    "520": "Two Family Dwelling Platted",
    "510": "One Family Dwelling Platted",
}

# Step 1: Read the files and extract address columns
file1 = pd.read_csv("Steps to Clean Raw Data/Scraped Parcel Files/PurdueOld.csv")
file2 = pd.read_csv("Steps to Clean Raw Data/Scraped Parcel Files/Updated_Property_Data_with_Address_Components.csv")
addresses = pd.concat([file1['Address'], file2['Address']]).drop_duplicates()

address_log = pd.DataFrame({
    "Address": addresses,
    "Status": "Not Scraped"  # Default status for all addresses
})

to_scrape = address_log[address_log['Status'] == "Not Scraped"]['Address'].tolist()
print(f"Addresses left to scrape: {len(to_scrape)}")
print(f"Initialized address log with {len(address_log)} addresses.")

def update_address_log(address, status, log_path="C:/Users/gabri/OneDrive/Desktop/Final Dashboard/Dashboard/Steps to Clean Raw Data/Scraped Parcel Files/PurdueStatusOutput.csv"):
    """
    Update the scrape status for a specific address and save to CSV.
    """
    global address_log  # Access the global log
    try:
        # Update the status in the DataFrame
        address_log.loc[address_log['Address'] == address, 'Status'] = status

        # Save the updated log to a CSV file
        address_log.to_csv(log_path, index=False)
        print(f"Updated log for address: {address} with status: {status}")
    except Exception as e:
        print(f"Error updating log for {address}: {e}")

# Step 2: Handle the pop up terms and conditions and make it automatically click the agree button
def handle_popup(driver):
    """Handle the Terms and Conditions popup if it appears."""
    try:
        # Wait for the popup to appear
        agree_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//a[contains(@class, "btn-primary") and text()="Agree"]'))
        )
        agree_button.click()
        print("Popup accepted.")
    except TimeoutException:
        print("No popup appeared. Continuing...")
    except Exception as e:
        print(f"Error handling popup: {e}")

# Step 3: Search Property function where we insert address into the search box and click search
def search_property(driver, address):
    try:
        # Wait for the search box and clear it
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//input[@id="ctlBodyPane_ctl02_ctl01_txtAddress"]'))
        )
        search_box.clear()
        search_box.send_keys(address)

        # Wait for and click the search button
        search_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, '//a[@id="ctlBodyPane_ctl02_ctl01_btnSearch"]'))
        )
        search_button.click()
        print(f"Searching for address: {address}")

        # Wait briefly for the page to respond
        time.sleep(3)

        # Check for direct navigation
        if driver.current_url and "PageTypeID=4" in driver.current_url:
            print(f"Direct navigation to property page detected for: {address}")
            return "direct_navigation"

        # Check for search results by verifying the presence of a results container
        search_results = driver.find_elements(By.XPATH, '//div[@class="module-content"]')
        if search_results:
            print(f"Search results page detected for: {address}")
            return "search_results"

        # If neither case, assume no results or an error
        print(f"No results or unexpected page for: {address}")
        return "no_results"

    except TimeoutException:
        print(f"Timeout while searching for address: {address}")
        return "error"
    except Exception as e:
        print(f"Error in search_property for address '{address}': {e}")
        return "error"

def reset_to_search_page(driver):
    try:
        search_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, '//li[@id="search1"]/a'))
        )
        driver.execute_script("arguments[0].click();", search_button)
        print("Reset to search page successfully.")
        time.sleep(1)
    except Exception as e:
        print(f"Error resetting to search page: {e}")
        driver.refresh()
        time.sleep(3)

temp_data = pd.DataFrame(columns=[
    "RentalID", "SalesID", "Address", "Beds", "Date", 
    "Price", "SQFT", "Transfer Type", "Instrument", 
    "Transfer To", "Property Classification", "Parcel ID", 
    "Acres", "Zoning Class", "Year Built", "Year Improved/Renovated", 
    "Grade", "Property Link", "Status"
])

def write_to_csv(entry, file_path='C:/Users/gabri/OneDrive/Desktop/Final Dashboard/Dashboard/Steps to Clean Raw Data/Scraped Parcel Files/PurduePropertyParcel9.csv'):
    headers = [
        "RentalID", "SalesID", "Address", "Beds", "Date", 
        "Price", "SQFT", "Transfer Type", "Instrument", 
        "Transfer To", "Property Classification", "Parcel ID", 
        "Acres", "Zoning Class", "Year Built", "Year Improved/Renovated", 
        "Grade", "Property Link", "Status"
    ]
    try:
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if csvfile.tell() == 0:  # Write the header only if the file is empty
                writer.writeheader()
            writer.writerow(entry)
        print(f"Entry for {entry.get('Address', 'Unknown Address')} written to CSV.")
    except Exception as e:
        print(f"Error writing to CSV: {e}")

def multiple_pages(driver, address):
    main_tab = driver.current_window_handle  # Store the handle of the main tab
    try:
        # Step 1: Check if "No Results Found" is displayed
        try:
            no_results = driver.find_element(By.XPATH, '//a[@id="ctlBodyPane_noDataList_lnkSearchPage"]')
            print(f"No results found for address: {address}. Resetting search...")
            no_result_entry = {
                "RentalID": None,
                "SalesID": None,
                "Address": address,
                "Beds": None,
                "Date": None,
                "Price": None,
                "SQFT": None,
                "Transfer Type": None,
                "Instrument": None,
                "Transfer To": None,
                "Property Classification": None,
                "Parcel ID": None,
                "Acres": None,
                "Zoning Class": None,
                "Year Built": None,
                "Year Improved/Renovated": None,
                "Grade": None,
                "Property Link": None,
                "Status": "Address Not Correct"
            }
            no_results.click()  # Return to the search page
            return [no_result_entry]  # Call centralized CSV-writing function
        except NoSuchElementException:
            pass# Results found, proceed

        # Step 2: Process multiple parcel links or single property
        parcel_links = driver.find_elements(By.XPATH, '//table[contains(@class, "")]/tbody/tr')
        if not parcel_links:
            print(f"No parcel links found for address: {address}")
            return []
        print(f"Found {len(parcel_links)} parcel links for address: {address}")

        # Extract Property Addresses for Filtering
        property_addresses = driver.find_elements(By.XPATH, './/table[contains(@class, "footable")]/tbody/tr/td[5]')  # Update XPath to match the address column
        if not property_addresses:
            print(f"No property addresses found in search results for address: {address}")
            return

        # Filter rows for exact address match
        matching_rows = []
        for idx, prop_address_element in enumerate(property_addresses, start=1):
            prop_address_text = prop_address_element.text.strip()
            if prop_address_text.lower() == address.lower():
                matching_rows.append(idx)
                print(f"Parcel {idx}: Address '{prop_address_text}' matches searched address '{address}'.")
            else:
                print(f"Parcel {idx}: Address '{prop_address_text}' does not match searched address '{address}'. Skipping...")

        if not matching_rows:
            print(f"No exact matches found for address: {address}. Skipping...")
            return

        # Step 3: Extract Class codes for Matching Rows
        valid_rows = []
        class_elements = driver.find_elements(By.XPATH, './/td[@align="center"]')  # Update XPath to match the class code column
        for idx in matching_rows:
            try:
                class_text = class_elements[idx - 1].text.strip()  # Use `idx - 1` because `matching_rows` is 1-indexed
                print(f"Extracted text from parcel {idx}: {class_text}")

                if class_text and class_text.split()[0] in class_codes:
                    class_code = class_text.split()[0]
                    valid_rows.append((idx, class_code))
                    print(f"Parcel {idx} has a valid class: {class_codes[class_code]} ({class_code}).")
                else:
                    print(f"Parcel {idx}: Invalid or missing class code. Skipping...")
            except Exception as e:
                print(f"Error extracting class code for parcel {idx}: {e}")

        if not valid_rows:
            print(f"No valid parcels found for address: {address}")
            return

        # Step 4: Expand Details for Valid Rows
        for idx, class_code in valid_rows:
            try:
                arrow_buttons = driver.find_elements(By.XPATH, '//a[contains(@class, "footable-toggle")]')
                if idx <= len(arrow_buttons):
                    arrow_buttons[idx - 1].click()
                    print(f"Parcel {idx}: Clicked arrow button to expand details.")
                    time.sleep(2)
                else:
                    print(f"Parcel {idx}: Arrow button not found. Skipping...")
                    continue
            except Exception as e:

                print(f"Parcel {idx}: Error clicking arrow button: {e}")
                continue

        # Step 5: Click on the Correct Property Link
        for idx, class_code in valid_rows:
            try:
                property_links = driver.find_elements(By.XPATH, '//a[contains(@class, "normal-font-label")]')
                if idx <= len(property_links):
                    property_links[idx - 1].click()
                    print(f"Parcel {idx}: Opened property link.")
                    time.sleep(2)
                    
                    property_link = driver.current_url
                    print(f"Extracted property link: {property_link}")

                    element_scrape(driver, address, property_link, rental_id_start=400000)
                else:
                    print(f"Parcel {idx}: Property link not found. Skipping...")
                    continue
            except Exception as e:
                print(f"Parcel {idx}: Unable to click property link. Error: {e}")
                continue
    except Exception as e:
        print(f"Error processing address '{address}': {e}")
        return []

def element_scrape(driver, address, property_link, rental_id_start = 400000):
    try:
        rental_id = rental_id_start  # Initialize RentalID
        transactions = []  # List to store all transactions for the property
        sales_id_start = 4000000

# 1. scrape the elements that are the same
        try:
            property_classification = driver.find_element(By.XPATH, '//table[contains(@class, "tabular-data-two-column")]//tr[11]//span').text.strip()
            parcel_id = driver.find_element(By.XPATH, '//table[contains(@class, "tabular-data-two-column")]//tr[1]//span').text.strip()
            property_acres = driver.find_element(By.XPATH, '//table[contains(@class, "tabular-data-two-column")]//tr[10]//span').text.strip()
            print(f"Property Classification: {property_classification}, Parcel ID: {parcel_id}, Acres: {property_acres}")
        except Exception as e:
            print(f"Error extracting general property details for {address}: {e}")
            return
# 2. Check for Transfers Section
        try:
            # Press Button to show all elements
            transfers_column = driver.find_element(By.XPATH, '//section[@id="ctlBodyPane_ctl20_mSection"]//button[contains(@class, "dropdown-toggle")]')
            driver.execute_script("arguments[0].click();", transfers_column)
            print("Dropdown toggle clicked to display options.")

            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, '//a[@role="menuitemcheckbox"]'))
            )
            print("Checkboxes are visible")
            time.sleep(2)

            checkboxes_to_toggle = [
            '//a[@role="menuitemcheckbox" and contains(text(), "To")]',
            '//a[@role="menuitemcheckbox" and contains(text(), "Sale Price")]'
            ]

            # Iterate over the checkbox items
            for checkbox_xpath in checkboxes_to_toggle:
                try:
                    # Locate and open the dropdown menu
                    transfers_column = driver.find_element(By.XPATH, '//section[@id="ctlBodyPane_ctl20_mSection"]//button[contains(@class, "dropdown-toggle")]')
                    driver.execute_script("arguments[0].click();", transfers_column)
                    print("Dropdown toggle reopened.")

                    # Locate the checkbox element
                    checkbox = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, checkbox_xpath))
                    )

                    # Check the current state of the checkbox using aria-checked attribute
                    aria_checked = checkbox.get_attribute("aria-checked")
                    if aria_checked == "true":
                        print(f"Checkbox '{checkbox_xpath}' is already checked. Skipping...")
                    else:
                        driver.execute_script("arguments[0].scrollIntoView({ behavior: 'smooth', block: 'center' });", checkbox)
                        time.sleep(0.5)
                        driver.execute_script("arguments[0].click();", checkbox)
                        print(f"Checkbox '{checkbox_xpath}' clicked successfully.")
                        time.sleep(1)
                except Exception as e:
                    print(f"Error interacting with checkbox '{checkbox_xpath}': {e}")

            print("Specified checkboxes are now checked.")
            # Scraping Date elements after clicking checkboxes
            valid_rows = []
            date_elements = driver.find_elements(By.XPATH, './/table[contains(@id, "ctlBodyPane_ctl20_ctl01_grdSales_grdFlat")]//tbody//tr//th')
            
            for idx, date_element in enumerate(date_elements, start=1):
                try:
                    date_text = date_element.text.strip()
                    transaction_date = datetime.strptime(date_text, "%m/%d/%Y")
                    cutoff_date = datetime.strptime("01/01/2000", "%m/%d/%Y")

                    if transaction_date >= cutoff_date:
                        valid_rows.append(idx)
                        print(f"Valid Row {idx}: Date={date_text}")
                    else:
                        print(f"Invalid Date (Before 2000): {date_text} (Row {idx})")
                except Exception as e:
                    print(f"Error processing Row {idx}: {e}")
            # Process the valid rows stored in `valid_rows`
            transactions = []
            for idx in valid_rows:
                try:
                    date_element = driver.find_element(
                        By.XPATH, f'//table[contains(@id, "ctlBodyPane_ctl20_ctl01_grdSales_grdFlat")]//tbody//tr[{idx}]//th[1]'
                    )
                    date_text = date_element.text.strip()
                    # Extract details for each valid row
                    type_of_transfer = driver.find_element(
                        By.XPATH, f'//table[contains(@id, "ctlBodyPane_ctl20_ctl01_grdSales_grdFlat")]//tbody//tr[{idx}]//td[2]'
                    ).text.strip()
                    instrument_transfer = driver.find_element(
                        By.XPATH, f'//table[contains(@id, "ctlBodyPane_ctl20_ctl01_grdSales_grdFlat")]//tbody//tr[{idx}]//td[3]'
                    ).text.strip()
                    transfer_to = driver.find_element(
                        By.XPATH, f'//table[contains(@id, "ctlBodyPane_ctl20_ctl01_grdSales_grdFlat")]//tbody//tr[{idx}]//td[5]'
                    ).text.strip()
                    sales_price = driver.find_element(
                        By.XPATH, f'//table[contains(@id, "ctlBodyPane_ctl20_ctl01_grdSales_grdFlat")]//tbody//tr[{idx}]//td[6]'
                    ).text.strip()
                    # Element scrape above
                    transaction = {
                        "RentalID": rental_id,  # Stays the same for the property
                        "SalesID": sales_id_start,  # Unique for each transaction
                        "Address": address,
                        "Beds": None,  # To be updated for residential properties
                        "Date": date_text,
                        "Price": sales_price,
                        "SQFT": None,  # To be updated for residential properties
                        "Transfer Type": type_of_transfer,
                        "Instrument": instrument_transfer,
                        "Transfer To": transfer_to,
                        "Property Classification": property_classification,
                        "Parcel ID": parcel_id,
                        "Acres": property_acres,
                        "Zoning Class": None,  # To be updated for residential properties
                        "Year Built": None,  # To be updated for residential properties
                        "Year Improved/Renovated": None,  # To be updated for residential properties
                        "Grade": None,  # To be updated for residential properties
                        "Property Link": property_link,
                        "Status": None
                    }
                    transactions.append(transaction)
                    print(f"Transaction for Row {idx}: {transaction}")
                    sales_id_start += 1
                except Exception as e:
                    print(f"Error extracting transaction details for Row {idx}: {e}")            
            if not transactions:
                minimal_transaction = {
                    "RentalID": rental_id,
                    "SalesID": None,
                    "Address": address,
                    "Beds": None,
                    "Date": None,
                    "Price": None,
                    "SQFT": None,
                    "Transfer Type": None,
                    "Instrument": None,
                    "Transfer To": None,
                    "Property Classification": property_classification,
                    "Parcel ID": parcel_id,
                    "Acres": property_acres,
                    "Zoning Class": None,
                    "Year Built": None,
                    "Year Improved/Renovated": None,
                    "Grade": None,
                    "Property Link": property_link,
                    "Status": None
                }
                transactions.append(minimal_transaction)
                print(f"No transactions for {address}. Minimal record added.")          
        except Exception as e:
            print(f"Transfers section not found for {address}: {e}")

# 2. Check if it is a residential property, if it is scrape more elements
        try:
            residential_property_xpath = (By.XPATH, '//section[@id="ctlBodyPane_ctl16_mSection"]')
                
            if residential_property_xpath:
                # For residential property we just have to extract this. 
                print(f"{address} is a residential property... Continue with residential scraping")
                zoning_classification = driver.find_element(By.XPATH, '//div[contains(@id,"ctlBodyPane_ctl16_ctl01_lstBuildings_ctl00_dynamicBuildingDataLeftColumn_rptrDynamicColumns_ctl00_pnlSingleValue")]').text.strip()
                year_built = driver.find_element(By.XPATH, '//div[contains(@id,"ctlBodyPane_ctl16_ctl01_lstBuildings_ctl00_dynamicBuildingDataLeftColumn_rptrDynamicColumns_ctl01_pnlSingleValue")]').text.strip()
                effective_year_built = driver.find_element(By.XPATH, '//div[contains(@id,"ctlBodyPane_ctl16_ctl01_lstBuildings_ctl00_dynamicBuildingDataLeftColumn_rptrDynamicColumns_ctl02_pnlSingleValue")]').text.strip()
                bedrooms = driver.find_element(By.XPATH, '//div[contains(@id,"ctlBodyPane_ctl16_ctl01_lstBuildings_ctl00_dynamicBuildingDataLeftColumn_rptrDynamicColumns_ctl05_pnlSingleValue")]').text.strip()
                square_footage = driver.find_element(By.XPATH, '//div[contains(@id,"ctlBodyPane_ctl16_ctl01_lstBuildings_ctl00_dynamicBuildingDataRightColumn_rptrDynamicColumns_ctl06_pnlSingleValue")]').text.strip()
                property_grade = driver.find_element(By.XPATH, '//div[contains(@id,"ctlBodyPane_ctl16_ctl01_lstBuildings_ctl00_dynamicBuildingDataRightColumn_rptrDynamicColumns_ctl11_pnlSingleValue")]').text.strip()

                for transaction in transactions:
                    transaction["Beds"] = bedrooms
                    transaction["Zoning Class"] = zoning_classification
                    transaction["Year Built"] = year_built
                    transaction["Year Improved/Renovated"] = effective_year_built
                    transaction["SQFT"] = square_footage
                    transaction["Grade"] = property_grade   
            else:
                print(f"{address} is a multi-family property... Ignore residential elements")
        except Exception as e:
                print(f"Error figuring out if {address} is a residential or multi-family property")
        return transactions  

    except Exception as overall_exception:
        print(f"Unexpected error during scraping for {address}: {overall_exception}")
        return []

def handle_captcha(driver):
    """
    Detect and handle CAPTCHA by pausing execution until the user solves it.
    """
    try:
        # Check for CAPTCHA presence by identifying the iframe or its container
        captcha_present = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//div[@class="g-recaptcha"]'))
        )
        if captcha_present:
            print("CAPTCHA detected. Please solve it manually.")
            input("Press Enter after solving the CAPTCHA to continue...")
            print("CAPTCHA solved. Resuming script...")
    except TimeoutException:
        # No CAPTCHA detected, continue as usual
        print("No CAPTCHA detected. Continuing script...")
    except Exception as e:
        print(f"Error detecting CAPTCHA: {e}")

def retry_property_search(driver, address, retry_attempts=3):
    """
    Retry the property search and scraping process if a CAPTCHA interrupts the flow.
    """
    attempts = 0
    while attempts < retry_attempts:
        try:
            # Handle CAPTCHA before retrying
            handle_captcha(driver)

            # Retry the search
            search_result = search_property(driver, address)

            if search_result == "direct_navigation":
                print(f"Retry {attempts + 1}: Directly scraping property for address: {address}")
                element_scrape(driver, address, driver.current_url)
                return True  # Successfully scraped

            elif search_result == "search_results":
                print(f"Retry {attempts + 1}: Processing search results for address: {address}")
                property_links = multiple_pages(driver, address)
                if property_links:
                    for property_link in property_links:
                        try:
                            driver.get(property_link)
                            print(f"Retry {attempts + 1}: Scraping property at: {property_link}")
                            element_scrape(driver, address, property_link)
                        except Exception as e:
                            print(f"Retry {attempts + 1}: Error processing property link '{property_link}' for address '{address}': {e}")
                    return True  # Successfully scraped
                else:
                    print(f"Retry {attempts + 1}: No valid property links found for address: {address}.")
                    return False  # No valid links

            elif search_result == "no_results":
                print(f"Retry {attempts + 1}: No results found for address: {address}.")
                return False  # No results

        except Exception as e:
            print(f"Retry {attempts + 1}: Error while retrying address '{address}': {e}")
            attempts += 1

    print(f"Exceeded maximum retries for address: {address}")
    return False  # Failed after retries

try:
    # Navigate to the main page
    driver.get('https://beacon.schneidercorp.com/Application.aspx?AppID=578&LayerID=8505&PageTypeID=2&PageID=4151')
    
    # Handle any popup that appears at the start
    handle_popup(driver)
    
    # Initialize RentalID for unique identification
    rental_id_start = 400000
    sales_id_start = 4000000

   # Main loop for processing addresses
    for address in to_scrape:
        try:
            # Reset to the search page before processing the address
            reset_to_search_page(driver)
            handle_captcha(driver)

            # Perform property search
            search_result = search_property(driver, address)

            # Initialize data to write as empty
            data_to_write = []

            if search_result == "direct_navigation":
                # Directly scrape the property
                print(f"Direct navigation for address: {address}")
                data_to_write = element_scrape(driver, address, driver.current_url, rental_id_start)
                rental_id_start += 1  # Increment RentalID for each property

            elif search_result == "search_results":
                # Process search results and scrape data
                print(f"Processing search results for address: {address}")
                data_to_write = multiple_pages(driver, address)

            elif search_result == "no_results":
                # Append no results entry if the property doesn't exist
                print(f"No results found for address: {address}")
                no_result_entry = {
                    "RentalID": rental_id_start,
                    "SalesID": None,
                    "Address": address,
                    "Beds": None,
                    "Date": None,
                    "Price": None,
                    "SQFT": None,
                    "Transfer Type": None,
                    "Instrument": None,
                    "Transfer To": None,
                    "Property Classification": None,
                    "Parcel ID": None,
                    "Acres": None,
                    "Zoning Class": None,
                    "Year Built": None,
                    "Year Improved/Renovated": None,
                    "Grade": None,
                    "Property Link": None,
                    "Status": "No Results"
                }
                data_to_write = [no_result_entry]
            else:
                # Handle unexpected search outcomes
                print(f"Unexpected result for address '{address}'. Retrying...")
                success = retry_property_search(driver, address)
                if success:
                    update_address_log(address, "Scraped After Retry")
                else:
                    update_address_log(address, "Failed After Retry")
                continue  # Move to the next address

            # Write data to CSV if available
            if data_to_write:
                for entry in data_to_write:
                    if entry:
                        write_to_csv(entry)
                update_address_log(address, "Scraped")
            else:
                print(f"No data found to write for address: {address}")
                update_address_log(address, "No Data")

        except Exception as e:
            # Log any errors for the specific address
            print(f"Error processing address '{address}': {e}")
            update_address_log(address, "Error")
            continue
finally:
    # Ensure the WebDriver quits at the end
    try:
        driver.quit()
    except Exception as e:
        print(f"Error during driver.quit(): {e}")
