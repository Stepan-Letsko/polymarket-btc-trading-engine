from dotenv import load_dotenv
from Order_client import build_client
from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

load_dotenv()

def print_balance():
    client = build_client()
    try:
        raw = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
    except Exception as e:
        print(f"Failed to fetch balance: {e}")
        return

    print(f"Raw response: {raw}\n")

    usdc = raw.get("balance")
    if usdc is not None:
        print(f"Available balance: ${float(usdc) / 1_000_000:.2f} USDC")

if __name__ == "__main__":
    print_balance()
