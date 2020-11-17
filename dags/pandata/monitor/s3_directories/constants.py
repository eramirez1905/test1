from dataclasses import dataclass
from typing import List

from configs.bigquery.tables.raw.adyen import ADYEN_TABLES
from configs.bigquery.tables.raw.billing import BILLING_TABLES
from configs.bigquery.tables.raw.corporate import CORPORATE_TABLES
from configs.bigquery.tables.raw.cybersource import CYBERSOURCE_TABLES
from configs.bigquery.tables.raw.intelligence_layer_contact_center import (
    INTELLIGENCE_LAYER_CCN_TABLES,
)
from configs.bigquery.tables.raw.images import IMAGES_TABLES
from configs.bigquery.tables.raw.joker import JOKER_TABLES
from configs.bigquery.tables.raw.loyalty import LOYALTY_TABLES
from configs.bigquery.tables.raw.payment import PAYMENT_TABLES
from configs.bigquery.tables.raw.pandora import PANDORA_TABLES
from configs.bigquery.tables.raw.refer_a_friend import REFER_A_FRIEND_TABLES
from configs.bigquery.tables.raw.reward import REWARD_TABLES
from configs.bigquery.tables.raw.subscription import SUBSCRIPTION_TABLES
from configs.bigquery.tables.raw.paypal import PAYPAL_TABLES
from load.s3.constants import (
    ADYEN_DIR,
    CYBERSOURCE_DIR,
    IL_REGION_CCN_AP_DIR,
    ML_ALF_AP_DIR,
    ML_BA_AP_DIR,
    ML_BE_AP_DIR,
    ML_BE_AP_INC_DIR,
    ML_CP_AP_DIR,
    ML_IMAGES_DIR,
    ML_JKR_AP_DIR,
    ML_LP_AP_DIR,
    ML_RAF_AP_DIR,
    ML_RW_AP_DIR,
    ML_SB_AP_DIR,
    PAYPAL_DIR,
)


@dataclass
class Check:
    parent_dir: str
    expected_child_dirs: List[str]


CHECKS = [
    Check(parent_dir=ADYEN_DIR, expected_child_dirs=[t.name for t in ADYEN_TABLES]),
    Check(
        parent_dir=CYBERSOURCE_DIR,
        expected_child_dirs=[t.name for t in CYBERSOURCE_TABLES]
    ),
    Check(
        parent_dir=IL_REGION_CCN_AP_DIR,
        expected_child_dirs=[t.name for t in INTELLIGENCE_LAYER_CCN_TABLES],
    ),
    Check(
        parent_dir=ML_ALF_AP_DIR, expected_child_dirs=[t.name for t in PAYMENT_TABLES]
    ),
    Check(
        parent_dir=ML_BE_AP_DIR,
        expected_child_dirs=[t.name for t in PANDORA_TABLES if t.is_full_load],
    ),
    Check(
        parent_dir=ML_BE_AP_INC_DIR,
        expected_child_dirs=[t.name for t in PANDORA_TABLES],
    ),
    Check(
        parent_dir=ML_BA_AP_DIR, expected_child_dirs=[t.name for t in BILLING_TABLES],
    ),
    Check(
        parent_dir=ML_CP_AP_DIR, expected_child_dirs=[t.name for t in CORPORATE_TABLES]
    ),
    Check(
        parent_dir=ML_IMAGES_DIR, expected_child_dirs=[t.name for t in IMAGES_TABLES]
    ),
    Check(parent_dir=ML_JKR_AP_DIR, expected_child_dirs=[t.name for t in JOKER_TABLES]),
    Check(
        parent_dir=ML_LP_AP_DIR, expected_child_dirs=[t.name for t in LOYALTY_TABLES]
    ),
    Check(
        parent_dir=ML_RAF_AP_DIR,
        expected_child_dirs=[t.name for t in REFER_A_FRIEND_TABLES],
    ),
    Check(parent_dir=ML_RW_AP_DIR, expected_child_dirs=[t.name for t in REWARD_TABLES]),
    Check(
        parent_dir=ML_SB_AP_DIR,
        expected_child_dirs=[t.name for t in SUBSCRIPTION_TABLES],
    ),
    Check(parent_dir=PAYPAL_DIR, expected_child_dirs=[t.name for t in PAYPAL_TABLES]),
]
