-- Primary Biliary Cirrhosis (PBC) Database Schema
-- Dataset from Fleming and Harrington (1991) - Mayo Clinic PBC Data
-- Used for liver transplant allocation AI research

CREATE TABLE IF NOT EXISTS pbc_patients (
    -- Patient identifier
    id INTEGER PRIMARY KEY,
    
    -- Survival/follow-up time in days from registration to death, transplant, or study end (July 1986)
    -- Critical for survival analysis - this is our primary outcome variable
    futime INTEGER NOT NULL,
    
    -- Patient status at end of follow-up period
    -- 0=alive (censored), 1=liver transplant (censored), 2=dead (event)
    status INTEGER NOT NULL,
    
    -- Treatment assignment in clinical trial
    -- 1=D-penicillamine (experimental drug), 2=placebo
    drug INTEGER,
    
    -- Patient age at registration in days (convert to years by dividing by 365.25)
    age INTEGER,
    
    -- Biological sex - important for MELD bias correction
    -- 0=male, 1=female
    sex INTEGER,
    
    -- Presence of ascites (fluid accumulation in abdomen)
    -- Indicator of advanced liver disease and portal hypertension
    -- 0=no, 1=yes
    ascites INTEGER,
    
    -- Presence of hepatomegaly (enlarged liver)
    -- Physical exam finding indicating liver dysfunction
    -- 0=no, 1=yes
    hepato INTEGER,
    
    -- Presence of spider angiomata (vascular lesions on skin)
    -- Clinical sign of chronic liver disease and portal hypertension
    -- 0=no, 1=yes
    spiders INTEGER,
    
    -- Presence and severity of edema (fluid retention)
    -- 0=no edema, 0.5=edema without/resolved with diuretics, 1=edema despite diuretics
    -- Indicates severity of liver dysfunction and fluid retention
    edema REAL,
    
    -- Serum bilirubin level in mg/dl
    -- Key component of MELD score, indicates liver's ability to process waste
    -- Higher values = worse liver function
    bili REAL,
    
    -- Serum cholesterol in mg/dl
    -- Can be elevated in liver disease, affects cardiovascular risk
    chol REAL,
    
    -- Serum albumin in gm/dl
    -- Protein made by liver, low levels indicate poor liver synthetic function
    -- Important predictor of survival in liver disease
    albumin REAL,
    
    -- 24-hour urine copper in ug/day
    -- Elevated in Wilson's disease and some liver conditions
    -- Important for differential diagnosis
    copper REAL,
    
    -- Alkaline phosphatase in U/liter
    -- Liver enzyme, elevated in cholestatic liver diseases like PBC
    -- Indicates bile duct damage or obstruction
    alk_phos REAL,
    
    -- SGOT (AST - Aspartate aminotransferase) in U/ml
    -- Liver enzyme released when liver cells are damaged
    -- Non-specific marker of liver injury
    sgot REAL,
    
    -- Triglycerides in mg/dl
    -- May be affected by liver disease and metabolic dysfunction
    trig REAL,
    
    -- Platelet count per cubic ml/1000
    -- Low platelets (thrombocytopenia) common in advanced liver disease
    -- Indicates portal hypertension and hypersplenism
    platelet REAL,
    
    -- Prothrombin time in seconds
    -- Key component of MELD score, measures blood clotting ability
    -- Elevated indicates poor liver synthetic function (makes clotting factors)
    protime REAL,
    
    -- Histologic stage of liver disease (1-4)
    -- 1=early disease, 4=advanced fibrosis/cirrhosis
    -- Determines disease severity and prognosis
    stage INTEGER,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_pbc_futime ON pbc_patients(futime);
CREATE INDEX IF NOT EXISTS idx_pbc_status ON pbc_patients(status);
CREATE INDEX IF NOT EXISTS idx_pbc_stage ON pbc_patients(stage);
CREATE INDEX IF NOT EXISTS idx_pbc_bili ON pbc_patients(bili);
CREATE INDEX IF NOT EXISTS idx_pbc_albumin ON pbc_patients(albumin);

-- View for survival analysis (only patients with complete data)
CREATE OR REPLACE VIEW pbc_survival_cohort AS
SELECT 
    id,
    futime as survival_days,
    CASE 
        WHEN status = 2 THEN 1 
        ELSE 0 
    END as death_event,
    age / 365.25 as age_years,
    sex,
    bili,
    albumin,
    protime,
    stage,
    ascites,
    edema,
    platelet
FROM pbc_patients
WHERE futime IS NOT NULL 
    AND status IS NOT NULL 
    AND bili IS NOT NULL 
    AND albumin IS NOT NULL 
    AND protime IS NOT NULL;

-- View for MELD score calculation (mimics current transplant allocation)
CREATE OR REPLACE VIEW pbc_with_meld AS
SELECT *,
    -- Simplified MELD-like score using available variables
    -- Note: Real MELD uses INR instead of protime, creatinine instead of some others
    ROUND(
        3.78 * LN(GREATEST(bili, 1.0)) + 
        11.2 * LN(GREATEST(protime/12.0, 1.0)) +  -- Approximate INR conversion
        9.57 * LN(GREATEST(2.0 - albumin, 1.0))   -- Proxy for creatinine using albumin
    ) as meld_like_score
FROM pbc_patients
WHERE bili > 0 AND protime > 0 AND albumin > 0;