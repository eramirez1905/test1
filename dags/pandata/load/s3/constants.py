from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
UTILS_DIR = CURRENT_DIR / "utils"
SQL_UTILS_DIR = UTILS_DIR / "sql"

DATAPROC_IMAGE_VERSION = "1.4"
DATAPROC_IDLE_DELETE_TTL = 900
DATAPROC_PROPERTIES = {"spark:spark.port.maxRetries": "256"}
DATAPROC_LOCATION = "us-central1"

S3_BUCKET_PANDORA = "pandora-export"
S3_BUCKET_NCR = "ncr-export"
S3_BUCKET_COMP_INTEL = "central-dwh-competitive-intelligence-export"
S3_BUCKET_MKT_CS = "mkt-cs-data-export-local"

ADYEN_DIR = "delta_payment/adyen_ap_export"
CLARISIGHTS_DIR = "clarisights/platform_name=pandora/company_name=foodpanda"
PAYPAL_DIR = "delta_payment/paypal_ap_export"
ML_ALF_AP_DIR = "delta_payment/ml_alf_ap_export"
CYBERSOURCE_DIR = "delta_payment/cybersource_export"

IL_REGION_AP_DIR = "delta_rs02/il_region_ap_export"
IL_REGION_CCN_AP_DIR = "delta_rs02/il_region_ccn_ap_export"
ML_BA_AP_DIR = "delta_rs02/ml_ba_ap_export"
ML_BE_AP_DIR = "delta_rs02/ml_be_ap_export"
ML_BE_AP_INC_DIR = "delta_rs02/ml_be_ap_export_inc"
ML_CP_AP_DIR = "delta_rs02/ml_cp_ap_export"
ML_IMAGES_DIR = "delta_rs02/ml_images_global_export"
ML_JKR_AP_DIR = "delta_rs02/ml_jkr_ap_export"
ML_LP_AP_DIR = "delta_rs02/ml_lp_ap_export"
ML_RAF_AP_DIR = "delta_rs02/ml_raf_ap_export"
ML_RW_AP_DIR = "delta_rs02/ml_rw_ap_export"
ML_SB_AP_DIR = "delta_rs02/ml_sb_ap_export"
RPS_AP_DIR = "rps_data"
