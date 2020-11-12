from dataclasses import dataclass
from configs.constructors.table import RawBigQueryTable, RawDataProcBigQueryTable


@dataclass(frozen=True)
class PandoraBigQueryTable(RawBigQueryTable):
    """
    All tables are incremental but those with full load
    have a separate schedule so that hard deletes are removed
    """

    is_full_load: bool = False


@dataclass(frozen=True)
class PandoraDataProcBigQueryTable(RawDataProcBigQueryTable):
    """
    All tables are incremental but those with full load
    have a separate schedule so that hard deletes are removed
    """

    is_full_load: bool = False


PANDORA_TABLES = (
    PandoraBigQueryTable(name="accounting"),
    PandoraBigQueryTable(name="accounting_vat_groups"),
    PandoraBigQueryTable(name="areas", is_full_load=True),
    PandoraBigQueryTable(name="basket_update", is_full_load=True),
    PandoraBigQueryTable(name="basket_update_product", is_full_load=True),
    PandoraBigQueryTable(name="basket_update_product_topping", is_full_load=True),
    PandoraBigQueryTable(name="blacklisted_phonenumbers", is_full_load=True),
    PandoraBigQueryTable(name="calls"),
    PandoraBigQueryTable(name="campaign", is_full_load=True),
    PandoraBigQueryTable(name="campaign_area", is_full_load=True),
    PandoraBigQueryTable(name="campaign_city", is_full_load=True),
    PandoraBigQueryTable(name="campaign_vendor", is_full_load=True),
    PandoraBigQueryTable(name="chain_menu_group", is_full_load=True),
    PandoraBigQueryTable(name="chain_voucher", is_full_load=True),
    PandoraBigQueryTable(name="choicetemplateproducts", is_full_load=True),
    PandoraBigQueryTable(name="choicetemplates", is_full_load=True),
    PandoraBigQueryTable(name="cities", is_full_load=True),
    PandoraBigQueryTable(name="cms", is_full_load=True),
    PandoraBigQueryTable(name="cms_content_link", is_full_load=True),
    PandoraBigQueryTable(name="configuration", is_full_load=True),
    PandoraBigQueryTable(name="cuisines", is_full_load=True),
    PandoraBigQueryTable(name="customeraddress"),
    PandoraBigQueryTable(name="customers"),
    PandoraBigQueryTable(name="deliveryprovider", is_full_load=True),
    PandoraBigQueryTable(name="discounts", is_full_load=True),
    PandoraBigQueryTable(name="discount_schedule", is_full_load=True),
    PandoraBigQueryTable(name="discountsattributions"),
    PandoraBigQueryTable(name="dish_characteristic", is_full_load=True),
    PandoraBigQueryTable(name="event", is_full_load=True),
    PandoraBigQueryTable(name="event_action", is_full_load=True),
    PandoraBigQueryTable(name="event_action_message", is_full_load=True),
    PandoraBigQueryTable(name="event_polygon", is_full_load=True),
    PandoraBigQueryTable(name="foodcaracteristics", is_full_load=True),
    PandoraBigQueryTable(name="foodpanda", is_full_load=True),
    PandoraBigQueryTable(name="foodpandaaddresses", is_full_load=True),
    PandoraBigQueryTable(name="fraud_validation_transaction"),
    PandoraBigQueryTable(name="globalcuisines", is_full_load=True),
    PandoraBigQueryTable(name="languages", is_full_load=True),
    PandoraBigQueryTable(name="loyaltyprogramnames", is_full_load=True),
    PandoraBigQueryTable(name="loyaltyprograms", is_full_load=True),
    PandoraBigQueryTable(name="master_categories", is_full_load=True),
    PandoraBigQueryTable(name="menucategories", is_full_load=True),
    PandoraBigQueryTable(name="menus", is_full_load=True),
    PandoraBigQueryTable(name="menusproducts"),
    PandoraBigQueryTable(name="newsletterusers", is_full_load=True),
    PandoraBigQueryTable(name="option_value", is_full_load=True),
    PandoraBigQueryTable(name="order_customer_additional_fields"),
    PandoraBigQueryTable(name="order_joker", is_full_load=True),
    PandoraBigQueryTable(name="order_payments"),
    PandoraBigQueryTable(name="order_related_time"),
    PandoraBigQueryTable(name="orderassignmentflows", is_full_load=True),
    PandoraBigQueryTable(name="orderdeclinereasons", is_full_load=True),
    PandoraBigQueryTable(name="orderproducts", is_full_load=True),
    PandoraBigQueryTable(name="orders"),
    PandoraBigQueryTable(name="ordertoppings", is_full_load=True),
    PandoraBigQueryTable(name="paymenttypes", is_full_load=True),
    PandoraBigQueryTable(name="products", is_full_load=True),
    PandoraBigQueryTable(name="productvariations", is_full_load=True),
    PandoraBigQueryTable(
        name="productvariations_dish_characteristic", is_full_load=True
    ),
    PandoraBigQueryTable(name="productvariationschoicetemplates", is_full_load=True),
    PandoraBigQueryTable(name="productvariationstoppingtemplates", is_full_load=True),
    PandoraBigQueryTable(name="roles", is_full_load=True),
    PandoraBigQueryTable(name="schedules", is_full_load=True),
    PandoraBigQueryTable(name="social_login", is_full_load=True),
    PandoraBigQueryTable(name="status", is_full_load=True),
    PandoraBigQueryTable(name="statusflows"),
    PandoraBigQueryTable(name="terms", is_full_load=True),
    PandoraBigQueryTable(name="toppingtemplateproducts"),
    PandoraBigQueryTable(name="toppingtemplates", is_full_load=True),
    PandoraBigQueryTable(name="translations", is_full_load=True),
    PandoraBigQueryTable(name="urlkeys", is_full_load=True),
    PandoraBigQueryTable(name="users", is_full_load=True),
    PandoraBigQueryTable(name="usersroles", is_full_load=True),
    PandoraBigQueryTable(name="vendor_configuration", is_full_load=True),
    PandoraBigQueryTable(name="vendorcontacts", is_full_load=True),
    PandoraBigQueryTable(name="vendordeliveries"),
    PandoraBigQueryTable(name="vendordeliveriesareas"),
    PandoraBigQueryTable(name="vendordeliveriespolygons"),
    PandoraBigQueryTable(name="vendordeliverytime"),
    PandoraBigQueryTable(name="vendorflows"),
    PandoraBigQueryTable(name="vendorreviews"),
    PandoraBigQueryTable(name="vendors_additional_info", is_full_load=True),
    PandoraBigQueryTable(name="vendorschains", is_full_load=True),
    PandoraBigQueryTable(name="vendorscuisines", is_full_load=True),
    PandoraBigQueryTable(name="vendorsdiscounts", is_full_load=True),
    PandoraBigQueryTable(name="vendorsfoodcaracteristics", is_full_load=True),
    PandoraBigQueryTable(name="vendorspaymenttypes"),
    PandoraBigQueryTable(name="vendorsscore", is_full_load=True),
    PandoraBigQueryTable(name="vendorsusers", is_full_load=True),
    PandoraBigQueryTable(name="vendorsvertical", is_full_load=True),
    PandoraBigQueryTable(name="vendorsvouchers"),
    PandoraBigQueryTable(name="voucherattributions"),
    PandoraBigQueryTable(name="whitelabel"),
    PandoraDataProcBigQueryTable(name="specialdays", is_full_load=True),
    PandoraDataProcBigQueryTable(name="vouchers"),
    PandoraDataProcBigQueryTable(name="vendors"),
)
