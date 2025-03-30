import os
import json
from dotenv import load_dotenv
from vnstock import Company

# Load environment variables from .env file
load_dotenv()

# Get environment variables
DATA_DIR = os.getenv("DATA_DIR")
COMPANY_INFO_FILE = os.getenv("COMPANY_INFO_FILE")

# Ensure the data directory exists
os.makedirs(DATA_DIR, exist_ok=True)
# Print the path to the company info file
print(f"Company info file path: {COMPANY_INFO_FILE}")
# Read the company info file and print the first 10 companies
if os.path.exists(COMPANY_INFO_FILE):
    with open(COMPANY_INFO_FILE, "r", encoding="utf-8") as f:
        company_info_list = json.load(f)
        for company in company_info_list[:10]:
            print(company)
else:
    print(f"Company info file {COMPANY_INFO_FILE} does not exist.")

# Test symbol
# symbol = 'YBM'
# company_info_list = []

# # Fetch company data
# company = Company(symbol=symbol)
# overview = company.overview()
# profile = company.profile()
# info = {
#     "symbol": symbol,
#     "exchange": overview.get("exchange")[0],
#     "industry": overview.get("industry")[0],
#     "company_type": overview.get("company_type")[0],
#     "established_year": overview.get("established_year")[0],
#     "stock_rating": overview.get("stock_rating")[0],
#     "short_name": overview.get("short_name")[0],
#     "website": overview.get("website")[0],
#     "company_name": profile.get("company_name")[0],
#     "company_profile": profile.get("company_profile")[0],
#     "history_dev": profile.get("history_dev")[0],
#     "company_promise": profile.get("company_promise")[0],
#     "business_risk": profile.get("business_risk")[0],
#     "key_developments": profile.get("key_developments")[0],
#     "business_strategies": profile.get("business_strategies")[0]
# }

# # Append the info to the list
# company_info_list.append(info)

# # Write to the JSON file
# with open(COMPANY_INFO_FILE, "w", encoding="utf-8") as f:
#     json.dump(company_info_list, f, ensure_ascii=False, indent=4)

# # Print success message
# print(f"Company info for {symbol} successfully written to {COMPANY_INFO_FILE}")