########################################################################
## In and Out of sample predictability of MSCI Barra Crowding Factors ##
#
# Final Project FIN 550 
# Spring 2021
# Cindy Lu
#
# Modified: 5/1/2021
########################################################################

#########
# Loading
#########
library(lubridate)
library(tidyverse)
library(zoo)
library(RPostgres)
library(data.table)
library(openxlsx)
library(scales)
library(viridis)    
library(moments)    
library(kableExtra) 
library(reshape2)
library(vroom)
library(sandwich)
library(lmtest)

############
# Login info
############

if (!exists("wrds")) {
  wrds <- dbConnect(Postgres(),
                    host='wrds-pgdata.wharton.upenn.edu',
                    port=9737,
                    dbname='wrds',
                    user=rstudioapi::askForPassword("Database username"),
                    password=rstudioapi::askForPassword("Database password"),
                    sslmode='require')
}

###########################
# Data gathering: Compustat 
###########################

# Retrieve Compustat data annual
res <- dbSendQuery(wrds,
                   "select GVKEY, CUSIP, DATADATE, FYR, FYEAR, SICH, NAICSH,
                  AT, LT, SEQ, CEQ, PSTKL, PSTKRV, PSTK, TXDITC, TXDB, ITCB,
                  REVT, COGS, XINT, XSGA, IB, TXDI, DVC, ACT, CHE, LCT,
                  DLC, TXP, DP, PPEGT, INVT, INDFMT, DATAFMT 
                  from COMP.FUNDA
                  where CONSOL='C' 
                  and POPSRC='D'")

# Query all records, typically set n = some maximum threshold
data.comp.funda <- dbFetch(res, n = -1)   # -1 for query all no limit

# Clear result query
dbClearResult(res)

#########################################
# Data gathering: CRSP Compustat linkfile
#########################################

# Get actual Compustat/CRSP linkfile
res <- dbSendQuery(wrds,
                   "select GVKEY, LPERMNO, LINKDT, LINKENDDT, LINKTYPE, LINKPRIM
                  from crsp.ccmxpf_lnkhist")

# Query linkfile, including all records
data.ccmlink <- dbFetch(res, n = -1) 

# Clear query
dbClearResult(res)

######################
# Data gathering: CRSP
######################

# Make query for CRSP MSF data
res <- dbSendQuery(wrds, 
                   "select DATE, PERMNO, PERMCO, CFACPR, CFACSHR, SHROUT, 
                   ALTPRC, PRC, RET, RETX, VOL
                   from CRSP.MSF")

# Return all entries
crsp.msf <- dbFetch(res, n = -1) 

# Second query for MSE data 
dbClearResult(res)
res <- dbSendQuery(wrds, 
                   "select DATE, PERMNO, SHRCD, SICCD, EXCHCD
                   from CRSP.MSE")

# Return all entries 
crsp.mse <- dbFetch(res, n = -1)

# Get entries from MSEDELIST dataset 
dbClearResult(res)
res <- dbSendQuery(wrds, "select DLSTDT, PERMNO, dlret, dlstcd
                   from crspq.msedelist")

# Query all 
crsp.msedelist <- dbFetch(res, n = -1)

##############################################
# Merging CRSP MSEDELIST, MSE and MSF datasets 
##############################################

# Cleaning CRSP MSF data 
crsp.msf <- crsp.msf %>%
  filter(!is.na(prc)) %>%               # delete unavailable prices
  mutate(Date = as.yearmon(as.Date(date))) %>%     # convert to Date
  select(-date)

# Cleaning MSE CRSP data similar steps 
crsp.mse <- crsp.mse %>%
  filter(!is.na(shrcd)) %>%
  mutate(Date = as.yearmon(as.Date(date))) %>%
  select(-date)

# Cleaning MSEDELIST data similarly 
crsp.msedelist <- crsp.msedelist %>%
  filter(!is.na(dlret)) %>%
  mutate(Date = as.yearmon(as.Date(dlstdt))) %>%
  select(-dlstdt)

# Merge on Date and permno for all 3 datasets 
data.crsp.msf <- crsp.msf %>%
  merge(crsp.mse,                    # merge with MSE first 
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  merge(crsp.msedelist,              # merge with MSEDELIST
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  rename(PERMNO=permno) %>%          # for consistency w Compustat
  
  # Change variables to factors
  #mutate_at(vars(PERMNO, permco, shrcd, exchcd), funs(as.factor)) %>%
  
  # Create new column, adjusted returns 
  mutate(retadj=ifelse(
    !is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) %>%
  
  # Sort by date and Permno, then group by permno stock identifier
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%    
  
  # Fill in NA with latest available 
  mutate_at(vars(shrcd, exchcd), funs(na.locf(., na.rm=FALSE)))  

# Clear some data from memory
rm(crsp.mse, crsp.msedelist, crsp.msf)

###############
# Cleaning CRSP 
###############

# Just to make sure it's consistent with Compustat
colnames(data.crsp.msf) <- str_to_lower(colnames(data.crsp.msf))

# Parse only relevant variables
tbl.crsp <- data.crsp.msf %>%
  transmute(permno = as.integer(permno),     # security identifier
            date = date,
            ret = as.numeric(ret) * 100,     # return (converted to percent)
            shrout = as.numeric(shrout),     # shares outstanding (thousands)
            altprc = as.numeric(altprc),     # last traded price in a month
            exchcd = as.integer(exchcd),     # exchange code
            shrcd = as.integer(shrcd),       # share code
            cfacshr = as.numeric(cfacshr),   # share count
            siccd = as.integer(siccd),       # industry code
            dlret = as.numeric(dlret) * 100, # delist return (convert to percent)
            dlstcd = as.integer(dlstcd)      # delisting code
  )

# Deal with the date!!! 
tbl.crsp$date <- as.Date(tbl.crsp$date)

# Analysis of fama french (1993) starts in 1963 so I only need data after 1962
tbl.crsp <- tbl.crsp %>%
  filter(date >= "1962-01-01")

# Keep only US-based common stocks (10 and 11)
tbl.crsp <- tbl.crsp %>%
  filter(shrcd %in% c(10, 11)) %>%
  select(-shrcd)

# No duplicates
tbl.crsp <- tbl.crsp %>%
  distinct(permno, date, .keep_all = TRUE) # remove duplicates 

# Compute market cap, altprc is negative average of bid and ask from last price
#       available if there is no last traded price
tbl.crsp <- tbl.crsp %>%
  mutate(mktcap = abs(shrout * altprc) / 1000,        # in millions of dollars
         mktcap = if_else(mktcap == 0, as.numeric(NA), mktcap)) 

# Define exchange labels and keep only NYSE, AMEX and NASDAQ stocks
tbl.crsp <- tbl.crsp %>%
  mutate(exchange = case_when(exchcd %in% c(1, 31) ~ "NYSE",
                              exchcd %in% c(2, 32) ~ "AMEX",
                              exchcd %in% c(3, 33) ~ "NASDAQ",
                              TRUE ~ "Other")) %>%
  filter(exchange != "Other")

# Adjust delisting returns (Shumway, 1997)
tbl.crsp <- tbl.crsp %>%
  mutate(ret_adj = case_when(is.na(dlstcd) ~ ret,
                             !is.na(dlstcd) & !is.na(dlret) ~ dlret,
                             dlstcd %in% c(500, 520, 580, 584) | 
                               (dlstcd >= 551 & dlstcd <= 574) ~ -30,
                             TRUE ~ -100)) %>%
  select(-c(dlret, dlstcd))

rm(data.crsp.msf)

################################
# Merging Compustat and linkfile
################################

data.ccm <- data.ccmlink %>%
  
  # Not all linktypes included, use standard ones
  filter(linktype %in% c("LU", "LC", "LS")) %>%
  
  # Use only 3 primary links
  filter(linkprim %in% c("P", "C", "J")) %>%
  
  # Inner join with compustat, only valid permno records kept
  merge(data.comp.funda, by="gvkey") %>% 
  
  # Renaming & reformat data types
  mutate(datadate = as.Date(datadate), 
         permno = as.factor(lpermno),
         linkdt = as.Date(linkdt),
         linkenddt = as.Date(linkenddt),
         linktype = factor(linktype, levels=c("LC", "LU", "LS")),
         linkprim = factor(linkprim, levels=c("P", "C", "J"))) %>%
  
  # Remove compustat fiscal ends later than linked end date
  filter(datadate >= linkdt & (datadate <= linkenddt | is.na(linkenddt))) %>%
  
  # Prioritize linktype, linkprim based on order of preference if duplicate
  arrange(datadate, permno, linktype, linkprim) %>%
  
  # Remove duplicates
  distinct(datadate, permno, .keep_all = TRUE)

# Clear 
rm(data.comp.funda, data.ccmlink)

######################
# Compustat processing
######################

# Just in case
colnames(data.ccm) <- str_to_lower(colnames(data.ccm))

# Select and parse relevant variables
tbl.compustat <- data.ccm %>%
  transmute(
    gvkey = as.integer(gvkey),         # firm identifier
    permno = as.integer(lpermno),      # stock identifier
    datadate = as.Date(datadate),      # date of report
    linktype = as.character(linktype), # link type
    linkenddt = ymd(linkenddt),        # date when link ends to be valid
    seq = as.numeric(seq),             # stockholders' equity
    ceq = as.numeric(ceq),             # total common/ordinary equity
    at = as.numeric(at),               # total assets
    lt = as.numeric(lt),               # total liabilities
    txditc = as.numeric(txditc),       # deferred taxes and investment tax credit
    txdb = as.numeric(txdb),           # deferred taxes
    itcb = as.numeric(itcb),           # investment tax credit
    pstkrv = as.numeric(pstkrv),       # preferred stock redemption value
    pstkl = as.numeric(pstkl),         # preferred stock liquidating value
    pstk = as.numeric(pstk),           # preferred stock par value
    indfmt = as.character(indfmt),     # industry format
    datafmt = as.character(datafmt)    # data format
  ) 

# Make sure that only correct industry and data format is used
tbl.compustat <- tbl.compustat %>%
  filter(indfmt == "INDL" & datafmt == "STD") %>%
  select(-c(indfmt, datafmt))

# Check that only valid links are used
tbl.compustat <- tbl.compustat %>%
  filter(linktype %in% c("LU", "LC"))

# Check that links are still active at time of information release
tbl.compustat <- tbl.compustat %>%
  filter(datadate <= linkenddt | is.na(linkenddt))

# Keep only distinct observations
tbl.compustat <- tbl.compustat %>%
  distinct()

# Calculate book value of preferred stock and equity
tbl.compustat <- tbl.compustat %>%
  mutate(be = coalesce(seq, ceq + pstk, at - lt) + 
           coalesce(txditc, txdb + itcb, 0) - 
           coalesce(pstkrv, pstkl, pstk, 0),
         be = if_else(be < 0, as.numeric(NA), be)) %>%
  select(gvkey, permno, datadate, be) 

# Determine year for matching and keep only last observation per year
tbl.compustat <- tbl.compustat %>%
  mutate(year = year(datadate)) %>%
  group_by(permno, year) %>%
  filter(datadate == max(datadate)) %>%
  ungroup()

# Keep only observations once a firm has been included for two years
tbl.compustat <- tbl.compustat %>%
  group_by(gvkey) %>%
  mutate(first_year = min(year)) %>%
  ungroup() %>%
  filter(year > first_year + 1) %>%
  select(-c(gvkey, first_year))

# Kick out unnecessary rows with missing values
tbl.compustat <- tbl.compustat %>%
  na.omit()

# Determine reference date for matching (i.e. June of next calendar year)
tbl.compustat <- tbl.compustat %>%
  mutate(reference_date = ymd(paste0(year + 1, "-06-01"))) %>%
  select(-year)

########################
# Construct stock sample
########################

# Select relevant variables
tbl.stocks <- tbl.crsp %>%
  select(permno, date, exchange, ret = ret_adj, mktcap) %>%
  na.omit()

# Define reference date each stock (i.e. new sort starts in June of year y)
tbl.stocks <- tbl.stocks %>%
  mutate(reference_date = ymd(if_else(month(date) < 6, 
                                      paste0(year(date) - 1, "-06-01"), 
                                      paste0(year(date), "-06-01")))) 

# Add BE data for year y-1 used starting in June of year y
tbl.stocks <- tbl.stocks %>%
  left_join(tbl.compustat, by = c("permno", "reference_date"))

# Add ME data from end of year y-1 used for bm ratio in June of year y
tbl.stocks_me <- tbl.stocks %>%
  filter(month(date) == 12) %>%
  mutate(reference_date = ymd(paste0(year(date) + 1, "-06-01"))) %>%
  select(permno, reference_date, me = mktcap)

tbl.stocks <- tbl.stocks %>%
  left_join(tbl.stocks_me, by = c("permno", "reference_date"))

# Compute book-to-market ratio
tbl.stocks <- tbl.stocks %>%
  mutate(bm = be / me)

# Add market cap of June of year y which is used for value-weighted returns
tbl.stocks_weight <- tbl.stocks %>%
  filter(month(date) == 6) %>%
  select(permno, reference_date, mktcap_weight = mktcap)

tbl.stocks <- tbl.stocks %>%
  left_join(tbl.stocks_weight, by = c("permno", "reference_date"))

# Put in lag return in stocks table
tbl.stocks <- tbl.stocks %>%
  group_by(permno) %>%
  mutate(ret.12t2 = ((lag(ret,2)+1)*(lag(ret,3)+1)*(lag(ret,4)+1)*
                       (lag(ret,5)+1)*(lag(ret,6)+1)*(lag(ret,7)+1)*
                       (lag(ret,8)+1)*
                       (lag(ret,9)+1)*(lag(ret,10)+1)*
                       (lag(ret,11)+1)*(lag(ret,12)+1))-1) %>%
  ungroup()

# Only keep stocks that have all the necessary data 
# (i.e. market equity data for December y-1 and June y, book equity data for y-1)
tbl.stocks <- tbl.stocks %>%
  na.omit() %>%
  filter(date >= "1961-01-01") # Fama-French paper starts here

###################
# Size decile sorts
###################

# In June of each year, all NYSE stocks are ranked on size to get the median
tbl.size_breakpoints <- tbl.stocks %>%
  filter(month(date) == 6 & exchange == "NYSE") %>%
  select(reference_date, mktcap) %>%
  group_by(reference_date) %>%
  summarize(size_median = median(mktcap))

# Also in June, all stocks are sorted into 2 portfolios
tbl.size_sorts <- tbl.stocks %>%
  filter(month(date) == 6) %>%
  left_join(tbl.size_breakpoints, by = "reference_date") %>%
  mutate(size_portfolio = case_when(mktcap > size_median ~ "B",
                                    mktcap <= size_median ~ "S",
                                    TRUE ~ as.character(NA))) %>%
  select(permno, reference_date, size_portfolio)

# Add size portfolio assignment back to stock data
tbl.stocks <- tbl.stocks %>% 
  left_join(tbl.size_sorts, by = c("permno", "reference_date"))

#############
# Value sorts
#############

# Calculate value breakpoints using NYSE stocks
tbl.value_breakpoints <- tbl.stocks %>%
  filter(month(date) == 6 & exchange == "NYSE") %>%
  select(reference_date, bm) %>%
  group_by(reference_date) %>%
  summarize(value_q30 = quantile(bm, 0.3),
            value_q70 = quantile(bm, 0.7))

# Also in June, all stocks are sorted into 3 portfolios
tbl.value_sorts <- tbl.stocks %>%
  filter(month(date) == 6) %>%
  left_join(tbl.value_breakpoints, by = "reference_date") %>%
  mutate(value_portfolio = case_when(bm > value_q70 ~ "H",
                                     bm <= value_q70 & bm > value_q30 ~ "M", 
                                     bm <= value_q30 ~ "L",
                                     TRUE ~ as.character(NA))) %>%
  select(permno, reference_date, value_portfolio)

# Add value portfolio assignment back to stock data
tbl.stocks <- tbl.stocks %>% 
  left_join(tbl.value_sorts, by = c("permno", "reference_date"))

########################################
# Quintile sorts for 2 crowding measures
########################################

# Calculate size breakpoints using all American exchange stocks
tbl.size_quintile_breakpoints <- tbl.stocks %>%
  
  # Use proper market capitalization date
  select(reference_date, mktcap) %>%
  group_by(reference_date) %>%
  summarize(size_q20 = quantile(mktcap, 0.2),
            size_q40 = quantile(mktcap, 0.4), 
            size_q60 = quantile(mktcap, 0.6), 
            size_q80 = quantile(mktcap, 0.8))

# Monthly stocks are sorted into quintiles
tbl.size_quintile_sorts <- tbl.stocks %>%
  left_join(tbl.size_quintile_breakpoints, by = "reference_date") %>%
  mutate(size_quintile_portfolio = case_when(mktcap > size_q80 ~ "5th",
                                             mktcap <= size_q80 & 
                                               mktcap > size_q60 ~ "4th", 
                                             mktcap <= size_q60 & 
                                               mktcap > size_q40 ~ "3rd",
                                             mktcap <= size_q40 & 
                                               mktcap > size_q20 ~ "2nd",
                                             mktcap <= size_q20 ~ "1st",
                                             TRUE ~ as.character(NA))) %>%
  select(permno, reference_date, size_quintile_portfolio) %>%
  distinct()

# Add value spread portfolio assignment back to stock data
tbl.stocks <- tbl.stocks %>%
  left_join(tbl.size_quintile_sorts, by = c("permno", "reference_date"))

#################
# Form portfolios
#################

tbl.portfolios <- tbl.stocks %>%
  group_by(date, size_portfolio, 
           value_portfolio) %>%
  summarize(ret_vw = weighted.mean(ret, mktcap_weight)) %>%
  ungroup() %>%
  mutate(portfolio_sv = paste0(size_portfolio, "/", value_portfolio))

tbl.factors <- tbl.portfolios %>%
  group_by(date) %>%
  summarize(smb = mean(ret_vw[portfolio_sv %in% c("S/H", "S/M", "S/L")]) - 
              mean(ret_vw[portfolio_sv %in% c("B/H", "B/M", "B/L")]),
            hml = mean(ret_vw[portfolio_sv %in% c("S/H", "B/H")]) - 
              mean(ret_vw[portfolio_sv %in% c("S/L", "B/L")]))

tbl.factors <- tbl.factors %>%
  left_join(tbl.stocks %>%
              group_by(date) %>%
              summarize(
                mkt = weighted.mean(ret, mktcap_weight)), by = "date"
  ) %>%
  
  # Rearrange columns
  select(date, mkt, smb, hml)

###############################
# Value spread crowding measure
###############################

tbl.factors$value_spread <- NA

for(i in 1:length(unique(tbl.stocks$date))){
  
  # Current date
  dt = unique(tbl.stocks$date)[i]
  
  # Highest quintile first
  df_5 <- tbl.stocks %>%
    filter(date == dt & size_quintile_portfolio == "5th")
  bm_median_high = median(df_5$bm)
  
  # Lowest quintile
  df_1 <- tbl.stocks %>%
    filter(date == dt & size_quintile_portfolio == "1st")
  bm_median_low <- median(df_1$bm)
  
  # Value spread formula by Barra
  val_spread <- log(bm_median_low / bm_median_high)
  
  # Save
  tbl.factors$value_spread[i] <- val_spread
}

####################################################
# Prepare to deal with memory issues with daily CRSP 
####################################################

# Export the unique permno's (stocks) from tbl.stocks
write.table(unique(tbl.stocks$permno), 
            "C://Users/cindy.lu/Downloads/permnos_use.txt", 
            append = FALSE, sep = "\t", dec = ".",
            row.names = FALSE, col.names = FALSE)

# Use above to hard - download daily CRSP file in 3 batches
pth1 <- "C://Users/cindy.lu/Downloads/crsp_dsf1.csv"
pth2 <- "C://Users/cindy.lu/Downloads/crsp_dsf2.csv"
pth3 <- "C://Users/cindy.lu/Downloads/crsp_dsf3.csv"

# Load one by one
tbl.crsp_dsf1 <- vroom(pth1, 
                       col_types = cols(.default = col_character()))
colnames(tbl.crsp_dsf1) <- str_to_lower(colnames(tbl.crsp_dsf1))

tbl.crsp_dsf2 <- vroom(pth2, 
                       col_types = cols(.default = col_character()))
colnames(tbl.crsp_dsf2) <- str_to_lower(colnames(tbl.crsp_dsf2))

tbl.crsp_dsf3 <- vroom(pth3, 
                       col_types = cols(.default = col_character()))
colnames(tbl.crsp_dsf3) <- str_to_lower(colnames(tbl.crsp_dsf3))

# Other 2 daily files
res <- dbSendQuery(wrds, 
                   "select DATE, PERMNO, SHRCD, SICCD
                   from CRSP.DSE")

# Return all entries 
crsp.dse <- dbFetch(res, n = -1)

# Get entries from DSEDELIST dataset 
dbClearResult(res)
res <- dbSendQuery(wrds, "select DLSTDT, PERMNO, dlret, dlstcd
                   from crspq.dsedelist")

# Query all and clear
crsp.dsedelist <- dbFetch(res, n = -1)
dbClearResult(res)

# Save DSE and DSEDELIST
saveRDS(crsp.dse, 
        "C://Users/cindy.lu/Downloads/crsp.dse.rds")
saveRDS(crsp.dsedelist, 
        "C://Users/cindy.lu/Downloads/crsp.dsedelist.rds")

# Reload
crsp.dse = readRDS("C://Users/cindy.lu/Downloads/crsp.dse.rds")
crsp.dsedelist = readRDS("C://Users/cindy.lu/Downloads/crsp.dsedelist.rds")

####################################
# Cleaning 1st batch daily CRSP data
####################################

# Cleaning CRSP DSF data file
crsp.dsf1 <- tbl.crsp_dsf1 %>%
  filter(!is.na(prc)) %>%               # delete unavailable prices
  mutate(Date = as.Date(as.character(date),
                        format = '%Y%m%d')) %>%     # convert to Date
  select(-date, -shrcd)
rm(tbl.crsp_dsf1)

# Cleaning DSE CRSP data similar steps 
crsp.dse <- crsp.dse %>%
  filter(!is.na(shrcd)) %>%
  mutate(Date = as.Date(date))%>%
  select(-date)

# Cleaning DSEDELIST data similarly 
crsp.dsedelist <- crsp.dsedelist %>%
  filter(!is.na(dlret)) %>%
  mutate(Date = as.Date(dlstdt)) %>%
  select(-dlstdt)

# Merge on Date and permno for all 3 datasets 
data.crsp.dsf1 <- crsp.dsf1 %>%
  merge(crsp.dse,                    # merge with DSE first 
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  merge(crsp.dsedelist,              # merge with DSEDELIST
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  rename(PERMNO=permno) %>%          # for consistency w Compustat
  
  # Create new column, adjusted returns 
  mutate(retadj=ifelse(
    !is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) %>%
  
  # Sort by date and Permno, then group by permno stock identifier
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%    
  
  # Fill in NA with latest available 
  mutate_at(vars(shrcd, exchcd), funs(na.locf(., na.rm=FALSE)))  

# Just to make sure it's consistent with Compustat
colnames(data.crsp.dsf1) <- str_to_lower(colnames(data.crsp.dsf1))

# Parse only relevant variables
tbl.crsp_daily1 <- data.crsp.dsf1 %>%
  transmute(permno = as.integer(permno),     # security identifier
            date = date,
            ret = as.numeric(ret) * 100,     # return (converted to percent)
            shrout = as.numeric(shrout),     # shares outstanding (thousands)
            prc = as.numeric(prc),           # last traded price in a month
            exchcd = as.integer(exchcd),     # exchange code
            shrcd = as.integer(shrcd),       # share code
            cfacshr = as.numeric(cfacshr),   # share count
            siccd = as.integer(siccd),       # industry code
            dlret = as.numeric(dlret) * 100, # delist return (convert to percent)
            dlstcd = as.integer(dlstcd)      # delisting code
  )

# Deal with the date!!! 
tbl.crsp_daily1$date <- as.Date(tbl.crsp_daily1$date)

# Keep only US-based common stocks (10 and 11)
tbl.crsp_daily1 <- tbl.crsp_daily1 %>%
  filter(shrcd %in% c(10, 11)) %>%
  select(-shrcd)

# No duplicates
tbl.crsp_daily1 <- tbl.crsp_daily1 %>%
  distinct(permno, date, .keep_all = TRUE) # remove duplicates 

# Analysis of fama french (1993) starts in 1963 so I only need data after 1962
tbl.crsp_daily1 <- tbl.crsp_daily1 %>%
  filter(date >= "1962-01-01")

# Just clean extra columns 
tbl.crsp_daily1 <- tbl.crsp_daily1 %>%
  select(-siccd, -dlret, -dlstcd)

# Save and clean some variables
saveRDS(tbl.crsp_daily1, 
        "C://Users/cindy.lu/Downloads/tbl.crsp_daily1.rds")
rm(data.crsp.dsf1, crsp.dsf1, tbl.crsp_daily1, res)  

####################################
# Cleaning 2nd batch daily CRSP data
####################################

# Cleaning CRSP DSF data file
crsp.dsf2 <- tbl.crsp_dsf2 %>%
  filter(!is.na(prc)) %>%               # delete unavailable prices
  mutate(Date = as.Date(as.character(date),
                        format = '%Y%m%d')) %>%     # convert to Date
  select(-date, -shrcd)
rm(tbl.crsp_dsf2)

# Merge on Date and permno, make subset due to memory problems 
cutoff_1 <- nrow(crsp.dsf2) %/% 3
data.crsp.dsf2_1 <- crsp.dsf2[1:cutoff_1, ] %>%
  merge(crsp.dse,                    # merge with DSE first 
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  merge(crsp.dsedelist,              # merge with DSEDELIST
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  rename(PERMNO=permno) %>%          # for consistency w Compustat
  
  # Create new column, adjusted returns 
  mutate(retadj=ifelse(
    !is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) %>%
  
  # Sort by date and Permno, then group by permno stock identifier
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%    
  
  # Fill in NA with latest available 
  mutate_at(vars(shrcd, exchcd), funs(na.locf(., na.rm=FALSE)))  

# Just to make sure consistent with Compustat
colnames(data.crsp.dsf2_1) <- str_to_lower(colnames(data.crsp.dsf2_1))

# Parse only relevant variables
tbl.crsp_daily2_1 <- data.crsp.dsf2_1 %>%
  transmute(permno = as.integer(permno),     # security identifier
            date = date,
            ret = as.numeric(ret) * 100,     # return (converted to percent)
            shrout = as.numeric(shrout),     # shares outstanding (thousands)
            prc = as.numeric(prc),           # last traded price in a month
            exchcd = as.integer(exchcd),     # exchange code
            shrcd = as.integer(shrcd),       # share code
            cfacshr = as.numeric(cfacshr),   # share count
            siccd = as.integer(siccd),       # industry code
            dlret = as.numeric(dlret) * 100, # delist return (convert to percent)
            dlstcd = as.integer(dlstcd)      # delisting code
  )

# Deal with the date!!! 
tbl.crsp_daily2_1$date <- as.Date(tbl.crsp_daily2_1$date)

# Keep only US-based common stocks (10 and 11)
tbl.crsp_daily2_1 <- tbl.crsp_daily2_1 %>%
  filter(shrcd %in% c(10, 11)) %>%
  select(-shrcd)

# No duplicates
tbl.crsp_daily2_1 <- tbl.crsp_daily2_1 %>%
  distinct(permno, date, .keep_all = TRUE) # remove duplicates 

# Fama-French 1993 starts in 1963 so only need data starting 1962
tbl.crsp_daily2_1 <- tbl.crsp_daily2_1 %>%
  filter(date >= "1962-01-01") %>%
  # Just clean extra columns  
  select(-siccd, -dlret, -dlstcd)

# Save and clean some variables
saveRDS(tbl.crsp_daily2_1, 
        "C://Users/cindy.lu/Downloads/tbl.crsp_daily2_1.rds")
rm() 

# Repeat for the second subset
cutoff_2 <- cutoff_1 * 2
data.crsp.dsf2_2 <- crsp.dsf2[(cutoff_1+1):cutoff_2, ] %>%
  merge(crsp.dse,                    # merge with DSE first 
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  merge(crsp.dsedelist,              # merge with DSEDELIST
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  rename(PERMNO=permno) %>%          # for consistency w Compustat
  
  # Create new column, adjusted returns 
  mutate(retadj=ifelse(
    !is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) %>%
  
  # Sort by date and Permno, then group by permno stock identifier
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%    
  
  # Fill in NA with latest available 
  mutate_at(vars(shrcd, exchcd), funs(na.locf(., na.rm=FALSE)))  

# Just to make sure consistent with Compustat
colnames(data.crsp.dsf2_2) <- str_to_lower(colnames(data.crsp.dsf2_2))

# Parse only relevant variables
tbl.crsp_daily2_2 <- data.crsp.dsf2_2 %>%
  transmute(permno = as.integer(permno),     # security identifier
            date = date,
            ret = as.numeric(ret) * 100,     # return (converted to percent)
            shrout = as.numeric(shrout),     # shares outstanding (thousands)
            prc = as.numeric(prc),           # last traded price in a month
            exchcd = as.integer(exchcd),     # exchange code
            shrcd = as.integer(shrcd),       # share code
            cfacshr = as.numeric(cfacshr),   # share count
            siccd = as.integer(siccd),       # industry code
            dlret = as.numeric(dlret) * 100, # delist return (convert to percent)
            dlstcd = as.integer(dlstcd)      # delisting code
  )

# Deal with the date!!! 
tbl.crsp_daily2_2$date <- as.Date(tbl.crsp_daily2_2$date)

# Keep only US-based common stocks (10 and 11)
tbl.crsp_daily2_2 <- tbl.crsp_daily2_2 %>%
  filter(shrcd %in% c(10, 11)) %>%
  select(-shrcd)

# No duplicates
tbl.crsp_daily2_2 <- tbl.crsp_daily2_2 %>%
  distinct(permno, date, .keep_all = TRUE)  

# Fama-French 1993 starts in 1963 so only need data starting 1962
tbl.crsp_daily2_2 <- tbl.crsp_daily2_2 %>%
  filter(date >= "1962-01-01") %>%
  # Just clean extra columns  
  select(-siccd, -dlret, -dlstcd)

# Save and clean some variables
saveRDS(tbl.crsp_daily2_2, 
        "C://Users/cindy.lu/Downloads/tbl.crsp_daily2_2.rds")
rm(tbl.crsp_daily2_2)

# 3rd subset
data.crsp.dsf2_3 <- crsp.dsf2[(cutoff_2+1):nrow(crsp.dsf2), ] %>%
  merge(crsp.dse,                    # merge with DSE first 
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  merge(crsp.dsedelist,              # merge with DSEDELIST
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  rename(PERMNO=permno) %>%          # for consistency w Compustat
  
  # Create new column, adjusted returns 
  mutate(retadj=ifelse(
    !is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) %>%
  
  # Sort by date and Permno, then group by permno stock identifier
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%    
  
  # Fill in NA with latest available 
  mutate_at(vars(shrcd, exchcd), funs(na.locf(., na.rm=FALSE)))  

# Just to make sure consistent with Compustat
colnames(data.crsp.dsf2_3) <- str_to_lower(colnames(data.crsp.dsf2_3))

# Parse only relevant variables
tbl.crsp_daily2_3 <- data.crsp.dsf2_3 %>%
  transmute(permno = as.integer(permno),     # security identifier
            date = date,
            ret = as.numeric(ret) * 100,     # return (converted to percent)
            shrout = as.numeric(shrout),     # shares outstanding (thousands)
            prc = as.numeric(prc),           # last traded price in a month
            exchcd = as.integer(exchcd),     # exchange code
            shrcd = as.integer(shrcd),       # share code
            cfacshr = as.numeric(cfacshr),   # share count
            siccd = as.integer(siccd),       # industry code
            dlret = as.numeric(dlret) * 100, # delist return (convert to percent)
            dlstcd = as.integer(dlstcd)      # delisting code
  )

# Deal with the date!!! 
tbl.crsp_daily2_3$date <- as.Date(tbl.crsp_daily2_3$date)

# Keep only US-based common stocks (10 and 11)
tbl.crsp_daily2_3 <- tbl.crsp_daily2_3 %>%
  filter(shrcd %in% c(10, 11)) %>%
  select(-shrcd)

# No duplicates
tbl.crsp_daily2_3 <- tbl.crsp_daily2_3 %>%
  distinct(permno, date, .keep_all = TRUE)  

# Fama-French 1993 starts in 1963 so only need data starting 1962
tbl.crsp_daily2_3 <- tbl.crsp_daily2_3 %>%
  filter(date >= "1962-01-01") %>%
  # Just clean extra columns  
  select(-siccd, -dlret, -dlstcd)

# Save and clean some variables
saveRDS(tbl.crsp_daily2_3, 
        "C://Users/cindy.lu/Downloads/tbl.crsp_daily2_3.rds")
rm(data.crsp.dsf2_2, data.crsp.dsf2_3)
rm(tbl.crsp_daily2_2, tbl.crsp_daily2_3, crsp.dsf2)

####################################
# Cleaning 3rd batch daily CRSP data
####################################

# Cleaning CRSP DSF data file
crsp.dsf3 <- tbl.crsp_dsf3 %>%
  filter(!is.na(prc)) %>%               # delete unavailable prices
  mutate(Date = as.Date(as.character(date),
                        format = '%Y%m%d')) %>%     # convert to Date
  select(-date, -shrcd)
rm(tbl.crsp_dsf3)

# Merge on Date and permno, make subset due to memory problems 
cutoff_1 <- nrow(crsp.dsf3) %/% 3
data.crsp.dsf3_1 <- crsp.dsf3[1:cutoff_1, ] %>%
  merge(crsp.dse,                    # merge with DSE first 
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  merge(crsp.dsedelist,              # merge with DSEDELIST
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  rename(PERMNO=permno) %>%          # for consistency w Compustat
  
  # Create new column, adjusted returns 
  mutate(retadj=ifelse(
    !is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) %>%
  
  # Sort by date and Permno, then group by permno stock identifier
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%    
  
  # Fill in NA with latest available 
  mutate_at(vars(shrcd, exchcd), funs(na.locf(., na.rm=FALSE)))  

# Just to make sure consistent with Compustat
colnames(data.crsp.dsf3_1) <- str_to_lower(colnames(data.crsp.dsf3_1))

# Parse only relevant variables
tbl.crsp_daily3_1 <- data.crsp.dsf3_1 %>%
  transmute(permno = as.integer(permno),     # security identifier
            date = date,
            ret = as.numeric(ret) * 100,     # return (converted to percent)
            shrout = as.numeric(shrout),     # shares outstanding (thousands)
            prc = as.numeric(prc),           # last traded price in a month
            exchcd = as.integer(exchcd),     # exchange code
            shrcd = as.integer(shrcd),       # share code
            cfacshr = as.numeric(cfacshr),   # share count
            siccd = as.integer(siccd),       # industry code
            dlret = as.numeric(dlret) * 100, # delist return (convert to percent)
            dlstcd = as.integer(dlstcd)      # delisting code
  )

# Deal with the date!!! 
tbl.crsp_daily3_1$date <- as.Date(tbl.crsp_daily3_1$date)

# Keep only US-based common stocks (10 and 11)
tbl.crsp_daily3_1 <- tbl.crsp_daily3_1 %>%
  filter(shrcd %in% c(10, 11)) %>%
  select(-shrcd)

# No duplicates
tbl.crsp_daily3_1 <- tbl.crsp_daily3_1 %>%
  distinct(permno, date, .keep_all = TRUE) # remove duplicates 

# Fama-French 1993 starts in 1963 so only need data starting 1962
tbl.crsp_daily3_1 <- tbl.crsp_daily3_1 %>%
  filter(date >= "1962-01-01") %>%
  # Just clean extra columns  
  select(-siccd, -dlret, -dlstcd)

# Save as RDS object
saveRDS(tbl.crsp_daily3_1, 
        "C://Users/cindy.lu/Downloads/tbl.crsp_daily3_1.rds")

# Repeat for the second subset
cutoff_2 <- cutoff_1 * 2
data.crsp.dsf3_2 <- crsp.dsf3[(cutoff_1+1):cutoff_2, ] %>%
  merge(crsp.dse,                    # merge with DSE first 
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  merge(crsp.dsedelist,              # merge with DSEDELIST
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  rename(PERMNO=permno) %>%          # for consistency w Compustat
  
  # Create new column, adjusted returns 
  mutate(retadj=ifelse(
    !is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) %>%
  
  # Sort by date and Permno, then group by permno stock identifier
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%    
  
  # Fill in NA with latest available 
  mutate_at(vars(shrcd, exchcd), funs(na.locf(., na.rm=FALSE)))  

# Just to make sure consistent with Compustat
colnames(data.crsp.dsf3_2) <- str_to_lower(colnames(data.crsp.dsf3_2))

# Parse only relevant variables
tbl.crsp_daily3_2 <- data.crsp.dsf3_2 %>%
  transmute(permno = as.integer(permno),     # security identifier
            date = date,
            ret = as.numeric(ret) * 100,     # return (converted to percent)
            shrout = as.numeric(shrout),     # shares outstanding (thousands)
            prc = as.numeric(prc),           # last traded price in a month
            exchcd = as.integer(exchcd),     # exchange code
            shrcd = as.integer(shrcd),       # share code
            cfacshr = as.numeric(cfacshr),   # share count
            siccd = as.integer(siccd),       # industry code
            dlret = as.numeric(dlret) * 100, # delist return (convert to percent)
            dlstcd = as.integer(dlstcd)      # delisting code
  )

# Deal with the date!!! 
tbl.crsp_daily3_2$date <- as.Date(tbl.crsp_daily3_2$date)

# Keep only US-based common stocks (10 and 11)
tbl.crsp_daily3_2 <- tbl.crsp_daily3_2 %>%
  filter(shrcd %in% c(10, 11)) %>%
  select(-shrcd)

# No duplicates
tbl.crsp_daily3_2 <- tbl.crsp_daily3_2 %>%
  distinct(permno, date, .keep_all = TRUE)  

# Fama-French 1993 starts in 1963 so only need data starting 1962
tbl.crsp_daily3_2 <- tbl.crsp_daily3_2 %>%
  filter(date >= "1962-01-01") %>%
  # Just clean extra columns  
  select(-siccd, -dlret, -dlstcd)

# Save and clean some variables
saveRDS(tbl.crsp_daily3_2, 
        "C://Users/cindy.lu/Downloads/tbl.crsp_daily3_2.rds")
rm(tbl.crsp_daily3_2, tbl.crsp_daily3_1, data.crsp.dsf3_1)

# 3rd subset
data.crsp.dsf3_3 <- crsp.dsf3[(cutoff_2+1):nrow(crsp.dsf3), ] %>%
  merge(crsp.dse,                    # merge with DSE first 
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  merge(crsp.dsedelist,              # merge with DSEDELIST
        by=c("Date", "permno"), 
        all=TRUE, 
        allow.cartesian=TRUE) %>%
  rename(PERMNO=permno) %>%          # for consistency w Compustat
  
  # Create new column, adjusted returns 
  mutate(retadj=ifelse(
    !is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) %>%
  
  # Sort by date and Permno, then group by permno stock identifier
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%    
  
  # Fill in NA with latest available 
  mutate_at(vars(shrcd, exchcd), funs(na.locf(., na.rm=FALSE)))  

# Just to make sure consistent with Compustat
colnames(data.crsp.dsf3_3) <- str_to_lower(colnames(data.crsp.dsf3_3))

# Parse only relevant variables
tbl.crsp_daily3_3 <- data.crsp.dsf3_3 %>%
  transmute(permno = as.integer(permno),     # security identifier
            date = date,
            ret = as.numeric(ret) * 100,     # return (converted to percent)
            shrout = as.numeric(shrout),     # shares outstanding (thousands)
            prc = as.numeric(prc),           # last traded price in a month
            exchcd = as.integer(exchcd),     # exchange code
            shrcd = as.integer(shrcd),       # share code
            cfacshr = as.numeric(cfacshr),   # share count
            siccd = as.integer(siccd),       # industry code
            dlret = as.numeric(dlret) * 100, # delist return (convert to percent)
            dlstcd = as.integer(dlstcd)      # delisting code
  )

# Deal with the date!!! 
tbl.crsp_daily3_3$date <- as.Date(tbl.crsp_daily3_3$date)

# Keep only US-based common stocks (10 and 11)
tbl.crsp_daily3_3 <- tbl.crsp_daily3_3 %>%
  filter(shrcd %in% c(10, 11)) %>%
  select(-shrcd)

# No duplicates
tbl.crsp_daily3_3 <- tbl.crsp_daily3_3 %>%
  distinct(permno, date, .keep_all = TRUE)  

# Fama-French 1993 starts in 1963 so only need data starting 1962
tbl.crsp_daily3_3 <- tbl.crsp_daily3_3 %>%
  filter(date >= "1962-01-01") %>%
  # Just clean extra columns  
  select(-siccd, -dlret, -dlstcd)

# Save and clean some variables
saveRDS(tbl.crsp_daily3_3, 
        "C://Users/cindy.lu/Downloads/tbl.crsp_daily3_3.rds")
rm(crsp.dse, crsp.dsedelist)
rm(data.crsp.dsf3_2, data.crsp.dsf3_3, crsp.dsf3, tbl.crsp_daily3_3)

# Reload and join all rows together for 2nd block
tbl.crsp_daily2 <- rbind(
  readRDS("C://Users/cindy.lu/Downloads/tbl.crsp_daily2_1.rds"), 
  readRDS("C://Users/cindy.lu/Downloads/tbl.crsp_daily2_2.rds"), 
  readRDS("C://Users/cindy.lu/Downloads/tbl.crsp_daily2_3.rds")
)

# Combine 2nd block with first
tbl.crsp_daily <- rbind(
  readRDS("C://Users/cindy.lu/Downloads/tbl.crsp_daily1.rds"), 
  tbl.crsp_daily2
)

# All subsections of the 3rd block
tbl.crsp_daily3 <- rbind(
  readRDS("C://Users/cindy.lu/Downloads/tbl.crsp_daily3_1.rds"), 
  readRDS("C://Users/cindy.lu/Downloads/tbl.crsp_daily3_2.rds"), 
  readRDS("C://Users/cindy.lu/Downloads/tbl.crsp_daily3_3.rds")
)

# Join all to create cleaned file of combined daily returns
tbl.crsp_daily <- rbind(tbl.crsp_daily, tbl.crsp_daily3)

# Last removal of empty rows
tbl.crsp_daily <- tbl.crsp_daily %>%
  filter(!is.na(ret))

# Save and clear
saveRDS(tbl.crsp_daily, 
        "C://Users/cindy.lu/Downloads/tbl.crsp_daily.rds")
rm(tbl.crsp_daily2, tbl.crsp_daily3)

###############################   
# Correlations crowding measure
###############################

# More clearing of variables
rm(data.ccm, res, tbl.compustat, tbl.crsp, tbl.stocks_me, tbl.stocks_weight, 
   tbl.value_breakpoints, tbl.value_sorts, tbl.size_sorts)

# Empty holding vector
tbl.factors$pairwise_corr_spread <- NA

# Loop over all dates contained in the factors data frame
for(i in 1:length(unique(tbl.stocks$date))){
  dt <- (tbl.factors$date)[i]

  # Work with top quintile first
  top_quint <- tbl.stocks %>%
    filter(size_quintile_portfolio == "5th" & date == dt)
  
  # Temporary data frame of daily returns 
  df <- tbl.crsp_daily %>%
    # Barra uses past 63 days returns 
    filter(date < dt & date >= as.Date(dt)-63) %>%
    # Among top quintile 
    filter(permno %in% unique(top_quint$permno))
  
  # Save correlations of stocks 
  corrs_high <- c()
  
  # Inner loop over all stocks in top quintile
  for(stock in unique(top_quint$permno)){
    # Returns of particular stock 
    ret_stock <- df %>%
      filter(permno == stock) %>%
      select(date, ret)
    # Temporary data frame for other top quintiles stocks
    temp <- df %>%
      # Kick out stock from above
      filter(permno != stock) %>%
      group_by(date) %>%
      
      # Average return each day in the 63-day window
      summarise(avg_rets = mean(ret))
    
    # Merge and append correlation computation
    ret_stock <- ret_stock %>%
      merge(temp, by="date")
    corrs_high <- c(corrs_high, cor(ret_stock$ret, ret_stock$avg_rets))
  }
  
  # Lowest quintile next
  bottom_quint <- tbl.stocks %>%
    filter(size_quintile_portfolio == "1st" & date == dt)
  
  # Temporary data frame of daily returns 
  df <- tbl.crsp_daily %>%
    # Barra uses past 63 days returns 
    filter(date < dt & date >= as.Date(dt)-63) %>%
    # Among top quintile 
    filter(permno %in% unique(bottom_quint$permno))
  
  # Save correlations of stocks 
  corrs_low <- c()
  
  # Inner loop over all stocks in lowest quintile
  for(stock in unique(bottom_quint$permno)){
    # Returns of particular stock 
    ret_stock <- df %>%
      filter(permno == stock) %>%
      select(date, ret)
    # Temporary data frame for other top quintiles stocks
    temp <- df %>%
      # Kick out stock from above
      filter(permno != stock) %>%
      group_by(date) %>%
      
      # Average return each day in the 63-day window
      summarise(avg_rets = mean(ret))
    
    # Merge and append correlation computation
    ret_stock <- ret_stock %>%
      merge(temp, by="date")
    corrs_low <- c(corrs_low, cor(ret_stock$ret, ret_stock$avg_rets))
  }

  # Append to main data frame
  tbl.factors$pairwise_corr_spread[i] <- mean(corrs_high, na.rm = TRUE) - mean(
    corrs_low, na.rm = TRUE)
}

# Clear memory 
rm(tbl.size_breakpoints, bottom_quint, top_quint, df, df_1, df_5, 
   tbl.crsp_daily, tbl.portfolios, tbl.size_quintile_breakpoints, 
   tbl.size_quintile_sorts, tbl.size_sorts, tbl.stocks, temp, ret_stock)

# Save the finalized factors file
saveRDS(tbl.factors, "C://Users/cindy.lu/Downloads/tbl.factors.csv")

###############################
# Replication correlations test
###############################

pth1 <- 'C://Users/cindy.lu/Downloads/F-F_Momentum_Factor.csv'
tbl.mom_ff <- read.csv(pth1, skip=13)
tbl.mom_ff <- tbl.mom_ff[1:1130,]

tbl.mom_ff <- tbl.mom_ff %>%
  transmute(date = as.Date(as.character(as.numeric(X)*100+1), 
                           format = '%Y%m%d'),
            mom_ff = as.numeric(Mom)) %>%
  filter(date <= max(tbl.crsp$date)) %>%
  mutate(date = floor_date(date, "month")) 

pth2 <- 'C://Users/cindy.lu/Downloads/F-F_Research_Data_Factors.csv'
tbl.factors_ff <- read.csv(pth2, skip=3)
tbl.factors_ff <- tbl.factors_ff[1:1134,]

tbl.factors_ff <- tbl.factors_ff %>%
  transmute(date = as.Date(as.character(as.numeric(X)*100+1), 
                           format = '%Y%m%d'),
            rf_ff = as.numeric(RF),
            mkt_ff = as.numeric(Mkt.RF) + rf_ff,
            smb_ff = as.numeric(SMB),
            hml_ff = as.numeric(HML)) %>%
  filter(date <= max(tbl.crsp$date)) %>%
  mutate(date = floor_date(date, "month")) %>%
  select(-rf_ff)

tbl.factors <- tbl.factors %>%
  mutate(date = floor_date(date, "month")) %>%
  left_join(tbl.factors_ff, by = "date") %>%
  left_join(tbl.mom_ff, by = "date")

#####################################################
# Informal investigation of distributional similarity 
#####################################################

tab.summary <- tbl.factors %>%
  select(-date) %>%
  pivot_longer(cols = everything(), names_to = "measure") %>%
  group_by(measure) %>%
  summarize(mean = mean(value),
            sd = sd(value),
            skew = skewness(value),
            kurt = kurtosis(value),
            min = min(value),
            q05 = quantile(value, 0.05),
            q25 = quantile(value, 0.25),
            q50 = quantile(value, 0.50),
            q75 = quantile(value, 0.75),
            q95 = quantile(value, 0.95),
            max = max(value),
            n = n()) %>%
  ungroup() %>%
  kable(digits = 2) %>%
  kable_styling(bootstrap_options = c("striped", "hover", 
                                      "condensed", "responsive"))
tab.summary

##########################
# Kolmogorov-Smirnov tests 
##########################

# Formally show no statistically significant difference in distributions
ks.test(tbl.factors$mkt, tbl.factors$mkt_ff)

ks.test(tbl.factors$smb, tbl.factors$smb_ff)

ks.test(tbl.factors$hml, tbl.factors$hml_ff)

ks.test(tbl.factors$mom, tbl.factors$mom_ff)

#####################
# Correlations matrix
#####################

fun.compute_correlations <- function(data, ..., upper = TRUE) {
  cor_matrix <- data %>%
    select(...) %>%
    cor(use = "complete.obs", method = "pearson") 
  if (upper == TRUE) {
    cor_matrix[upper.tri(cor_matrix, diag = FALSE)] <- NA
  }
  return(cor_matrix)
}
tab.correlations <- fun.compute_correlations(tbl.factors, 
                                             mkt, mkt_ff, smb, 
                                             smb_ff, hml, hml_ff) %>% 
  kable(digits = 2) %>%
  kable_styling(bootstrap_options = c("striped", "hover", 
                                      "condensed", "responsive"))
tab.correlations

# Get rid of last few columns
tbl.factors <- tbl.factors %>%
  select(-mkt_ff, -smb_ff, -hml_ff)

#################################################
# UPDATE: adding momentum factor (J & T, Carhart)
#################################################

# Compustat annual 
res <- dbSendQuery(wrds,"select GVKEY, CUSIP, DATADATE, FYR, FYEAR, SICH, NAICSH,
                   AT, LT, SEQ, CEQ, PSTKL, PSTKRV, PSTK, TXDITC, TXDB, ITCB,
                   REVT, COGS, XINT, XSGA, IB, TXDI, DVC, ACT, CHE, LCT,
                   DLC, TXP, DP, PPEGT, INVT
                   from COMP.FUNDA
                   where INDFMT='INDL' and DATAFMT='STD' and 
                   CONSOL='C' and POPSRC='D'") 
data.comp.funda <- dbFetch(res, n = -1) 
save(data.comp.funda, file = "180619 data.comp.funda.RData")

# retrieve Compustat quarterly data
res <- dbSendQuery(wrds,"select GVKEY, CUSIP, DATADATE, FYR, FYEARQ,
                   ATQ, LTQ, SEQQ, CEQQ, PSTKRQ, PSTKQ, TXDITCQ, TXDBQ,
                   REVTQ, COGSQ, XINTQ, XSGAQ, IBQ, TXDIQ, ACTQ, CHEQ, LCTQ,
                   DLCQ, TXPQ, DPQ, PPEGTQ, INVTQ, EPSPXQ, RDQ
                   from COMPM.FUNDQ
                   where INDFMT='INDL' and DATAFMT='STD' and 
                   CONSOL='C' and POPSRC='D'") # STD is unrestatd data
data.comp.fundq <- dbFetch(res, n = -1) 
save(data.comp.fundq, file = "180619 data.comp.fundq.RData")

# retrieve Merged Compustat/CRSP link table
res <- dbSendQuery(wrds,"select GVKEY, LPERMNO, LINKDT, LINKENDDT, 
LINKTYPE, LINKPRIM
                   from crsp.ccmxpf_lnkhist")
data.ccmlink <- dbFetch(res, n = -1)
save(data.ccmlink, file = "180619 data.ccmlink.RData")

# Merge linked permno and Compustat datasets
data.ccm <-  data.ccmlink %>%
  # Use valid primary links 
  filter(linktype %in% c("LU", "LC", "LS")) %>%
  filter(linkprim %in% c("P", "C", "J")) %>%
  # Keep only if permno exists
  merge(data.comp.funda, by="gvkey") %>% 
  mutate(datadate = as.Date(datadate),
         permno = as.factor(lpermno),
         linkdt = as.Date(linkdt),
         linkenddt = as.Date(linkenddt),
         linktype = factor(linktype, levels=c("LC", "LU", "LS")),
         linkprim = factor(linkprim, levels=c("P", "C", "J"))) %>%
  
  # Delete compustat fiscal ends outside link period  
  filter(datadate >= linkdt & (datadate <= linkenddt | 
                                 is.na(linkenddt))) %>%
  arrange(datadate, permno, linktype, linkprim) %>%
  distinct(datadate, permno, .keep_all = TRUE)
save(data.ccm, file = "180619 data.ccm.RData")
rm(data.comp.funda, data.ccmlink)

# Compustat cleaning initial
load("180619 data.ccm.RData")
data.comp <- data.ccm %>%
  rename(PERMNO=permno) %>% data.table %>% 
  group_by(PERMNO) %>%
  mutate(datadate = as.yearmon(datadate),
         comp.count = n()) %>% 
  ungroup %>% arrange(datadate, PERMNO) %>% data.frame %>%
  # Just in case get all unique rows
  distinct(datadate, PERMNO, .keep_all = TRUE) 

data.comp.a <- data.comp %>%
  group_by(PERMNO) %>%
  mutate(BE = coalesce(seq, ceq + pstk, at - lt) + 
           coalesce(txditc, txdb + itcb, 0) - coalesce(pstkrv, pstkl, pstk, 0), 
         OpProf = (revt - coalesce(cogs, 0) - coalesce(xint, 0) - 
                     coalesce(xsga,0)),
         OpProf = as.numeric(ifelse(is.na(cogs) & is.na(xint) & is.na(xsga), 
                                    NA, OpProf)), 
         GrProf = (revt - cogs),
         Cflow = ib + coalesce(txdi, 0) + dp,  
         Inv = (coalesce(ppegt - lag(ppegt), 0) + 
                  coalesce(invt - lag(invt), 0)) / lag(at),
         AstChg = (at - lag(at)) / lag(at) 
         # Note: lags use previous available (may have distance of 1 year)
  ) %>% ungroup %>%
  arrange(datadate, PERMNO) %>%
  select(datadate, PERMNO, comp.count, at, revt, ib, dvc, BE:AstChg) %>%
  mutate_if(is.numeric, funs(ifelse(is.infinite(.), NA, .))) %>% 
  mutate_if(is.numeric, funs(round(., 5)))        # control rounding

# Save and clear annual compustat files
save(data.comp.a, file="180619 data.comp.a.RData")
rm(data.ccm, data.comp)

# Load CRSP files from wrds
res <- dbSendQuery(wrds, "select DATE, PERMNO, PERMCO, CFACPR, CFACSHR, 
                   SHROUT, PRC, RET, RETX, VOL
                   from CRSP.MSF")
crsp.msf <- dbFetch(res, n = -1)
save(crsp.msf, file = "180619 crsp.msf.RData")

res <- dbSendQuery(wrds, "select DATE, PERMNO, SHRCD, EXCHCD
                   from CRSP.MSE")
crsp.mse <- dbFetch(res, n = -1)
save(crsp.mse, file = "180619 crsp.mse.RData")

res <- dbSendQuery(wrds, "select DLSTDT, PERMNO, dlret
                   from crspq.msedelist")
crsp.msedelist <- dbFetch(res, n = -1)
save(crsp.msedelist, file = "180619 crsp.msedelist.RData")

# Further merging
crsp.msf <- crsp.msf %>%
  filter(!is.na(prc)) %>%
  mutate(Date = as.yearmon(as.Date(date))) %>%
  select(-date)
crsp.mse <- crsp.mse %>%
  filter(!is.na(shrcd)) %>%
  mutate(Date = as.yearmon(as.Date(date))) %>%
  select(-date)
crsp.msedelist <- crsp.msedelist %>%
  filter(!is.na(dlret)) %>%
  mutate(Date = as.yearmon(as.Date(dlstdt))) %>%
  select(-dlstdt)
data.crsp.m <- crsp.msf %>%
  merge(crsp.mse, by=c("Date", "permno"), all=TRUE, 
        allow.cartesian=TRUE) %>%
  merge(crsp.msedelist, by=c("Date", "permno"), all=TRUE, 
        allow.cartesian=TRUE) %>%
  rename(PERMNO=permno) %>%
  mutate_at(vars(PERMNO, permco, shrcd, exchcd), funs(as.factor)) %>%
  mutate(retadj=ifelse(!is.na(ret), ret, 
                       ifelse(!is.na(dlret), dlret, NA))) %>% 
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%  
  
  # Impute NA's with last available (sorted by Date, group by PERMNO)
  mutate_at(vars(shrcd, exchcd), funs(na.locf(., na.rm=FALSE)))  

data.crsp.m <- data.crsp.m %>%
  # Get rid of negative signs 
  mutate(meq = shrout * abs(prc)) %>% 
  group_by(Date, permco) %>%
  
  # In market cap calculation, merge permnos with permnco
  mutate(ME = sum(meq)) %>% 
  arrange(Date, permco, desc(meq)) %>%
  group_by(Date, permco) %>%
  
  # Keep largest market equity for each permno
  slice(1) %>% 
  ungroup

# Clear all intermediate CRSP download files
save(data.crsp.m, file = "180619 data.crsp.m.RData")
rm(crsp.mse, crsp.msf, crsp.msedelist)

# Takes dataframe with Date and PERMNO columns and fills gaps with NA
Fill_TS_NAs <- function(main) {
  core <- select(main, Date, PERMNO)
  # Get start and last dates of each PERMNO
  date.bookends <- core %>%
    group_by(PERMNO) %>%
    summarize(first = first(Date), last = last(Date))
  
  # Get all start-end dates for PERMNOs, delete if outside each range
  output <- core %>%
    # Create temp 3rd column just to use spread
    mutate(temp = 1) %>% 
    spread(., PERMNO, temp) %>%
    gather(., PERMNO, temp, -Date) %>%
    merge(date.bookends, by="PERMNO", all.x=TRUE) %>%
    group_by(PERMNO) %>%
    filter(Date>=first & Date<=last) %>%
    select(Date, PERMNO) %>%
    merge(., main, by=c("Date", "PERMNO"), all.x=TRUE)
  
  return(output)
}

# Load in CRSP data so far
load("180619 data.crsp.m.RData")
data.crsp.cln <- data.crsp.m %>%
  select(Date, PERMNO, shrcd, exchcd, cfacpr, cfacshr, shrout, 
         prc, vol, retx, retadj, ME) %>%
  # Match compustat, convert thousands to millions 
  mutate(ME = ME/1000) %>%  
  filter((shrcd == 10 | shrcd == 11) & (
    exchcd == 1 | exchcd == 2 | exchcd == 3)) %>%
  # Gaps in dates for each PERMNO filled with NA 
  Fill_TS_NAs %>% 
  mutate(PERMNO = as.factor(PERMNO)) %>%
  group_by(PERMNO) %>%
  # Weights are ME at start of period
  mutate(port.weight = as.numeric(ifelse(!is.na(lag(ME)), lag(ME), 
                                         ME/(1+retx))), 
         port.weight = ifelse(is.na(retadj) & is.na(prc), 
                              NA, port.weight)) %>% 
  # remove portweights calc for date gaps
  ungroup %>%
  rename(retadj.1mn = retadj) %>%
  arrange(Date, PERMNO) %>%
  # Just in case need to filter out duplicates
  distinct(Date, PERMNO, .keep_all = TRUE) 

# Save cleaner version of CRSP
save(data.crsp.cln, file = "180619 data.crsp.cln.RData")
rm(data.crsp.m)

# Load before merging again CRSP and Compustst
load("180619 data.crsp.cln.RData")
load("180619 data.comp.a.RData")

# Fill "tops" of time-series data with NA until first valid observation
na_locf_until = function(x, n) {
  l <- cumsum(! is.na(x))
  c(NA, x[! is.na(x)])[replace(l, ave(l, l, FUN=seq_along) > (n+1), 0) + 1]
}

data.both.m <- data.comp.a %>%  
  # Map to next year June when data is known 
  mutate(Date = datadate + (18-month(datadate))/12) %>% 
  
  # Keep all CRSP records (Compustat only starts in 1950)
  merge(data.crsp.cln, ., by=c("Date", "PERMNO"), all.x=TRUE, 
        allow.cartesian=TRUE) %>%  
  arrange(PERMNO, Date, desc(datadate)) %>%
  distinct(PERMNO, Date, .keep_all = TRUE) %>% 
  # drop older datadates (must sort by desc(datadate))
  group_by(PERMNO) %>%
  ungroup %>%
  mutate(datadate = yearmon(datadate)) %>%
  arrange(Date, PERMNO)

# Save and remove 
save(data.both.m, file = "180619 data.both.m.RData")
rm(data.comp.a, data.crsp.cln)

# Loading
load("180619 data.both.m.RData")
data.both.FF.m <- data.both.m %>%
  group_by(PERMNO) %>%
  
  # change in monthly share count (adjusted for splits)
  mutate(d.shares = (shrout*cfacshr)/(lag(shrout)*lag(cfacshr))-1, 
         # For momentum calculation 
         ret.12t2 = (lag(retadj.1mn,1)+1)*(lag(retadj.1mn,2)+1)*(
           lag(retadj.1mn,3)+1)*(lag(retadj.1mn,4)+1)*
           (lag(retadj.1mn,5)+1)*(lag(retadj.1mn,6)+1)*(
             lag(retadj.1mn,7)+1)*(lag(retadj.1mn,8)+1)*
           (lag(retadj.1mn,9)+1)*(lag(retadj.1mn,10)+1)*(
             lag(retadj.1mn,11)+1)-1, 
         BE = coalesce(BE), 
         # Data available by June based on Compustat date mapping
         ME.Dec = as.numeric(ifelse(month(Date)==6 & lag(ME,6)>0, 
                                    lag(ME,6), NA)), 
         ME.Jun = as.numeric(ifelse(month(Date)==6, ME, NA)),
         BM.FF = as.numeric(ifelse(month(Date)==6 & ME.Dec>0, BE/ME.Dec, NA)),
         OpIB = as.numeric(ifelse(month(Date)==6 & BE>0, OpProf/BE, NA)),
         GrIA = as.numeric(ifelse(month(Date)==6 & at>0, GrProf/at, NA)),
         CFP.FF = as.numeric(ifelse(month(Date)==6 & ME.Dec>0, 
                                    Cflow/ME.Dec, NA)),
         BM.m = BE/ME,          # monthly updated version for spread 
         CFP.m = Cflow/ME, 
         lag.ME.Jun = lag(ME.Jun), 
         lag.BM.FF = lag(BM.FF),
         lag.ret.12t2 = lag(ret.12t2),
         lag.OpIB = lag(OpIB),
         lag.AstChg = lag(AstChg))

data.both.FF.m %<>%
  # code Inf values as NAs
  mutate_at(vars(d.shares:lag.AstChg), 
            funs(ifelse(!is.infinite(.), ., NA))) %>% 
  select(Date, datadate, PERMNO, exchcd, comp.count, prc, 
         vol, retadj.1mn, d.shares, ME, port.weight,
         ret.12t2, at:AstChg, ME.Jun:lag.AstChg) %>%
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%
  mutate_at(vars(ME.Jun:CFP.FF, lag.ME.Jun:lag.AstChg), 
            funs(na_locf_until(., 11))) %>%
  ungroup %>%
  mutate(port.weight = ifelse(is.na(port.weight), 0, port.weight)) 
save(data.both.FF.m, file = "180619 data.both.FF.m.RData")
rm(data.both.m)

# For restarting...
temp <- data.both.FF.m

# FF Factors formation
Form_CharSizePorts2 <- function(main, size, var, wght, ret) {
  # forms 2x3 (size x variable) 6 portfolios
  main.cln <- main %>%
    select(Date, PERMNO, exchcd, !!size, !!var, !!wght, !!ret)
  
  # create size and var breakpoints based on NYSE stocks only
  Bkpts.NYSE <- main.cln %>% 
    filter(exchcd == 1) %>% # NYSE exchange
    group_by(Date) %>%
    # Variable/momentum broken by 30-70 percentile
    summarize(var.P70 = quantile(!!var, probs=.7, na.rm=TRUE),
              var.P30 = quantile(!!var, probs=.3, na.rm=TRUE),
              size.Med = quantile(!!size, probs=.5, na.rm=TRUE))
  
  # calculate size and var portfolio returns
  main.rank <- main.cln %>%
    merge(Bkpts.NYSE, by="Date", all.x=TRUE) %>%
    mutate(Size = ifelse(!!size<size.Med, "Small", "Big"),
           Var = ifelse(!!var<var.P30, "Low", 
                        ifelse(!!var>var.P70, "High", "Neutral")),
           Port = paste(Size, Var, sep="."))
  
  Ret <- main.rank %>% # name 2 x 3 size-var portfolios
    group_by(Date, Port) %>%
    summarize(ret.port = weighted.mean(!!ret, !!wght, na.rm=TRUE)) %>% 
    # Transposition of portfolios from row to columns
    spread(Port, ret.port) %>% 
    mutate(Small = (Small.High + Small.Neutral + Small.Low)/3,
           Big = (Big.High + Big.Neutral + Big.Low)/3,
           SMB = Small - Big,
           High = (Small.High + Big.High)/2,
           Low = (Small.Low + Big.Low)/2,
           HML = High - Low)
  
  return(Ret)
}

Form_FF6Ports <- function(df) {
  # form FF6 factors from data (SMB, HML, RMW, CMA, UMD)
  output <- df %>%
    group_by(Date) %>%
    summarize(MyMkt = weighted.mean(retadj.1mn, 
                                    w=port.weight, na.rm=TRUE)) %>%
    merge(Form_CharSizePorts2(df, quo(lag.ME.Jun), quo(lag.BM.FF), 
                              quo(port.weight), quo(retadj.1mn)),
          by="Date", all.x=TRUE) %>% # SMB, HML
    select(Date:MyMkt, MySMB=SMB, MySMBS=Small, MySMBB=Big, 
           MyHML=HML, MyHMLH=High, MyHMLL=Low) %>%
    merge(Form_CharSizePorts2(df, quo(lag.ME.Jun), quo(lag.OpIB), 
                              quo(port.weight), quo(retadj.1mn)),
          by="Date", all.x=TRUE) %>% # RMW
    select(Date:MyHMLL, MyRMW=HML, MyRMWR=High, MyRMWW=Low) %>%
    merge(Form_CharSizePorts2(df, quo(lag.ME.Jun), quo(lag.AstChg), 
                              quo(port.weight), quo(retadj.1mn)),
          by="Date", all.x=TRUE) %>% # CMA
    select(Date:MyRMWW, MyCMA=HML, MyCMAC=Low, MyCMAA=High) %>%
    mutate(MyCMA=-MyCMA) %>%
    merge(Form_CharSizePorts2(df, quo(port.weight), quo(lag.ret.12t2), 
                              quo(port.weight), quo(retadj.1mn)),
          by="Date", all.x=TRUE) %>% # UMD
    select(Date:MyCMAA, MyUMD=HML, MyUMDU=High, MyUMDD=Low)
  return(output)
}

# Reload this data 
load("180619 data.both.FF.m.RData")
dt.myFF6.m <- Form_FF6Ports(data.both.FF.m) %>%
  filter(year(Date) >= 1961) %>%
  mutate(date = as.Date(Date), umd = MyUMD) %>%
  select(date, umd)

# Save a copy
save(dt.myFF6.m, file = "180619 dt.myFF6.m.RData")

# Read in file with UMD data 
ff_actual <- read.xlsx(
  "C://Users/cindy.lu/Downloads/Fama French Factors Data.xlsx")
ff_actual <- ff_actual %>%
  mutate(date = as.Date(as.character(Date*100 + 1),
                        format = '%Y%m%d')) %>%
  select(-Date)

# Put in actual FF factors
joined <- dt.myFF6.m %>%
  merge(ff_actual, by = "date")

# Take correlation separately from everything else, > 0.999
cor(joined$umd*100, joined$UMD)

# Join with tbl.factors
tbl.factors$date <- as.Date(tbl.factors$date)
tbl.factors <- tbl.factors %>%
  left_join(dt.myFF6.m, by = "date")

# Save and load again before regressions 
saveRDS(tbl.factors, "C://Users/cindy.lu/Downloads/tbl.factors.rds")
readRDS("C://Users/cindy.lu/Downloads/tbl.factors.rds")
# Save and load again before regressions 
tbl.factors <- readRDS("C://Users/cindy.lu/Downloads/tbl.factors.rds")

#########################################################
# Run predictive forecast regressions, in-sample analysis
#########################################################

# Code up the regressions predicting each FF factor return 
js <- c(1, 3, 6, 12)

for(j in js){
  
  # Predictive regression of smb factor
  pred_reg_smb <- lm(
    tbl.factors$smb[(1+j):nrow(tbl.factors)] ~ 
      tbl.factors$smb[1:(nrow(tbl.factors)-j)] + 
      tbl.factors$value_spread[1:(nrow(tbl.factors)-j)] + 
      tbl.factors$pairwise_corr_spread[1:(nrow(tbl.factors)-j)] +
      tbl.factors$umd[1:(nrow(tbl.factors)-j)]
  )
  
  # HML regression
  pred_reg_hml <- lm(
    tbl.factors$hml[(1+j):nrow(tbl.factors)] ~ 
      tbl.factors$hml[1:(nrow(tbl.factors)-j)] + 
      tbl.factors$value_spread[1:(nrow(tbl.factors)-j)] + 
      tbl.factors$pairwise_corr_spread[1:(nrow(tbl.factors)-j)] +
      tbl.factors$umd[1:(nrow(tbl.factors)-j)]
  )
  
  # Save smb regression coefficients
  coeffs_smb <- data.frame(regressor = c(
    "intercept", "smb lag", "value spread", "pairwise correlation spread", 
    "umd"))
  coeffs_smb$betas <- pred_reg_smb$coefficients
  if(j > 1){
    NW_VCOV <- NeweyWest(pred_reg_smb, 
                         lag = j - 1, prewhite = F, 
                         adjust = T)
    coeffs_smb$std_errs <- coeftest(pred_reg_smb, vcov = NW_VCOV)[,2]
  } else {
    coeffs_smb$std_errs <- summary(pred_reg_smb)$coef[,2]
  }
  coeffs_smb$t_stats <- abs(coeffs_smb$betas)/coeffs_smb$std_errs
  cat('smb results', 'j =', j, '\n\n')
  print(coeffs_smb)
  cat('\n')
  
  # Save hml regression coefficients
  coeffs_hml <- data.frame(regressor = c(
    "intercept", "hml lag", "value spread", "pairwise correlation spread", 
    "umd"))
  coeffs_hml$betas <- pred_reg_hml$coefficients
  if(j > 1){
    NW_VCOV <- NeweyWest(pred_reg_hml, 
                         lag = j - 1, prewhite = F, 
                         adjust = T)
    coeffs_hml$std_errs <- coeftest(pred_reg_hml, vcov = NW_VCOV)[,2]
  } else{
    coeffs_hml$std_errs <- summary(pred_reg_hml)$coef[,2]
  }
  coeffs_hml$t_stats <- abs(coeffs_hml$betas)/coeffs_hml$std_errs
  cat('HML results', 'j =', j, '\n\n')
  print(coeffs_hml)
  cat('\n')
}

##############################################################
# Generate simulated out-of-sample HML excess return forecasts
##############################################################

for(j in js){
  tot = nrow(tbl.factors)
  # Start index of the out-of-sample data 
  start = which(tbl.factors$date == "2014-01-01")
  
  # Storage matrix for realized values & forecasts
  forecast_hml = data.frame(matrix(NA, (tot-1)-(start-1)+1, 4))
  colnames(forecast_hml) = c('actual',
                             'combined',
                             'bench1_AR', 
                             'bench2_crowd')
  forecast_hml$actual <- tbl.factors$hml[start:tot]
  
  preds_crowd <- c()
  preds_combined <- c()
  preds_AR <- c()
  
  for(s in (start-1):(tot-1)){
    df <- tbl.factors %>%
      filter(date <= tbl.factors$date[s]) %>%
      select(date, hml, value_spread, pairwise_corr_spread, umd)
    combined_mod <- lm(
      df$hml[(1+j):nrow(df)] ~ 
        df$hml[1:(nrow(df)-j)] + 
        df$value_spread[1:(nrow(df)-j)] + 
        df$pairwise_corr_spread[1:(nrow(df)-j)] +
        df$umd[1:(nrow(df)-j)]
    )
    AR_mod <- lm(df$hml[(1+j):nrow(df)] ~ df$hml[1:(nrow(df)-j)] + 
                   df$umd[1:(nrow(df)-j)])
    crowd_mod <- lm(
      df$hml[(1+j):nrow(df)] ~ df$value_spread[1:(nrow(df)-j)] + 
        df$pairwise_corr_spread[1:(nrow(df)-j)] + df$umd[1:(nrow(df)-j)]
    )
    
    new_pred_combined <- sum(df[s,-1] * combined_mod$coefficients[-1]) + 
      combined_mod$coefficients[1]
    new_pred_AR <- sum(df[s,-1] * AR_mod$coefficients[-1]) + 
      AR_mod$coefficients[1]
    new_pred_crowd <- sum(df[s,-1] * crowd_mod$coefficients[-1]) + 
      crowd_mod$coefficients[1]
    
    preds_combined <- c(preds_combined, new_pred_combined)
    preds_AR <- c(preds_AR, new_pred_AR)
    preds_crowd <- c(preds_crowd, new_pred_crowd)
  }
  
  forecast_hml$combined <- preds_combined
  forecast_hml$bench1_AR <- preds_AR
  forecast_hml$bench2_crowd <- preds_crowd
  
  # MSFE for benchmark AR forecast
  msfeBenchAR = mean((forecast_hml$actual-forecast_hml$bench1_AR)^2)
  
  # MSFE for benchmark Crowding forecast
  msfeBenchCrowd = mean((forecast_hml$actual-forecast_hml$bench2_crowd)^2)
  
  # MSFE for combined predictive regression forecast
  msfePr = mean((forecast_hml$actual-forecast_hml$combined)^2)
  
  # MSFE ratios (less than one => PR forecast more accurate)
  msfeRatio_vsAR = msfePr/msfeBenchAR
  msfeRatio_vsCrowd = msfePr/msfeBenchCrowd
  
  # Print information
  cat('MSFEs, j =', j, ', hml factor returns', '\n\n')
  print(c("MSFE(combined)/MSFE(AR) = ", msfeRatio_vsAR))
  cat('\n')
  print(c("MSFE(combined)/MSFE(crowd) = ", msfeRatio_vsCrowd))
  cat('\n')
}

##############################################################
# Generate simulated out-of-sample SMB excess return forecasts
##############################################################

for(j in js){
  tot = nrow(tbl.factors)
  # Start index of the out-of-sample data 
  start = which(tbl.factors$date == "2014-01-01")
  
  # Storage matrix for realized values & forecasts
  forecast_smb = data.frame(matrix(NA, (tot-1)-(start-1)+1, 4))
  colnames(forecast_smb) = c('actual',
                             'combined',
                             'bench1_AR', 
                             'bench2_crowd')
  forecast_smb$actual <- tbl.factors$smb[start:tot]
  
  preds_crowd <- c()
  preds_combined <- c()
  preds_AR <- c()
  
  for(s in (start-1):(tot-1)){
    df <- tbl.factors %>%
      filter(date <= tbl.factors$date[s]) %>%
      select(date, smb, value_spread, pairwise_corr_spread, umd)
    combined_mod <- lm(
      df$smb[(1+j):nrow(df)] ~ 
        df$smb[1:(nrow(df)-j)] + 
        df$value_spread[1:(nrow(df)-j)] + 
        df$pairwise_corr_spread[1:(nrow(df)-j)] +
        df$umd[1:(nrow(df)-j)]
    )
    AR_mod <- lm(df$smb[(1+j):nrow(df)] ~ df$smb[1:(nrow(df)-j)] + 
                   df$umd[1:(nrow(df)-j)])
    crowd_mod <- lm(
      df$smb[(1+j):nrow(df)] ~ df$value_spread[1:(nrow(df)-j)] + 
        df$pairwise_corr_spread[1:(nrow(df)-j)] + df$umd[1:(nrow(df)-j)]
    )
    
    new_pred_combined <- sum(df[s,-1] * combined_mod$coefficients[-1]) + 
      combined_mod$coefficients[1]
    new_pred_AR <- sum(df[s,-1] * AR_mod$coefficients[-1]) + 
      AR_mod$coefficients[1]
    new_pred_crowd <- sum(df[s,-1] * crowd_mod$coefficients[-1]) + 
      crowd_mod$coefficients[1]
    
    preds_combined <- c(preds_combined, new_pred_combined)
    preds_AR <- c(preds_AR, new_pred_AR)
    preds_crowd <- c(preds_crowd, new_pred_crowd)
  }
  
  forecast_smb$combined <- preds_combined
  forecast_smb$bench1_AR <- preds_AR
  forecast_smb$bench2_crowd <- preds_crowd
  
  # MSFE for benchmark AR forecast
  msfeBenchAR = mean((forecast_smb$actual-forecast_smb$bench1_AR)^2)
  
  # MSFE for benchmark Crowding forecast
  msfeBenchCrowd = mean((forecast_smb$actual-forecast_smb$bench2_crowd)^2)
  
  # MSFE for combined predictive regression forecast
  msfePr = mean((forecast_smb$actual-forecast_smb$combined)^2)
  
  # MSFE ratios (less than one => PR forecast more accurate)
  msfeRatio_vsAR = msfePr/msfeBenchAR
  msfeRatio_vsCrowd = msfePr/msfeBenchCrowd
  
  # Print information
  cat('MSFEs, j =', j, ', smb factor returns', '\n\n')
  print(c("MSFE(combined)/MSFE(AR) = ", msfeRatio_vsAR))
  cat('\n')
  print(c("MSFE(combined)/MSFE(crowd) = ", msfeRatio_vsCrowd))
  cat('\n')
}

##################################
# Plot forecasts & realized values
##################################

# Just choose HML, j=12 and re-run
for(j in c(12)){
  tot = nrow(tbl.factors)
  # Start index of the out-of-sample data 
  start = which(tbl.factors$date == "2014-01-01")
  
  # Storage matrix for realized values & forecasts
  forecast_hml = data.frame(matrix(NA, (tot-1)-(start-1)+1, 4))
  colnames(forecast_hml) = c('actual',
                             'combined',
                             'bench1_AR', 
                             'bench2_crowd')
  forecast_hml$actual <- tbl.factors$hml[start:tot]
  
  preds_crowd <- c()
  preds_combined <- c()
  preds_AR <- c()
  
  for(s in (start-1):(tot-1)){
    df <- tbl.factors %>%
      filter(date <= tbl.factors$date[s]) %>%
      select(date, hml, value_spread, pairwise_corr_spread, umd)
    combined_mod <- lm(
      df$hml[(1+j):nrow(df)] ~ 
        df$hml[1:(nrow(df)-j)] + 
        df$value_spread[1:(nrow(df)-j)] + 
        df$pairwise_corr_spread[1:(nrow(df)-j)] +
        df$umd[1:(nrow(df)-j)]
    )
    AR_mod <- lm(df$hml[(1+j):nrow(df)] ~ df$hml[1:(nrow(df)-j)] + 
                   df$umd[1:(nrow(df)-j)])
    crowd_mod <- lm(
      df$hml[(1+j):nrow(df)] ~ df$value_spread[1:(nrow(df)-j)] + 
        df$pairwise_corr_spread[1:(nrow(df)-j)] + df$umd[1:(nrow(df)-j)]
    )
    
    new_pred_combined <- sum(df[s,-1] * combined_mod$coefficients[-1]) + 
      combined_mod$coefficients[1]
    new_pred_AR <- sum(df[s,-1] * AR_mod$coefficients[-1]) + 
      AR_mod$coefficients[1]
    new_pred_crowd <- sum(df[s,-1] * crowd_mod$coefficients[-1]) + 
      crowd_mod$coefficients[1]
    
    preds_combined <- c(preds_combined, new_pred_combined)
    preds_AR <- c(preds_AR, new_pred_AR)
    preds_crowd <- c(preds_crowd, new_pred_crowd)
  }
  
  forecast_hml$combined <- preds_combined
  forecast_hml$bench1_AR <- preds_AR
  forecast_hml$bench2_crowd <- preds_crowd
  
  # MSFE for benchmark AR forecast
  msfeBenchAR = mean((forecast_hml$actual-forecast_hml$bench1_AR)^2)
  
  # MSFE for benchmark Crowding forecast
  msfeBenchCrowd = mean((forecast_hml$actual-forecast_hml$bench2_crowd)^2)
  
  # MSFE for combined predictive regression forecast
  msfePr = mean((forecast_hml$actual-forecast_hml$combined)^2)
  
  # MSFE ratios (less than one => PR forecast more accurate)
  msfeRatio_vsAR = msfePr/msfeBenchAR
  msfeRatio_vsCrowd = msfePr/msfeBenchCrowd
  
  # Print information
  cat('MSFEs, j =', j, ', hml factor returns', '\n\n')
  print(c("MSFE(combined)/MSFE(AR) = ", msfeRatio_vsAR))
  cat('\n')
  print(c("MSFE(combined)/MSFE(crowd) = ", msfeRatio_vsCrowd))
  cat('\n')
}

# Reshape data frame for figure
forecast_hml$date <- tbl.factors$date[which(tbl.factors$date >= "2014-01-01")]
forecast = melt(forecast_hml, id.vars = 'date')

# Create figure
fi = ggplot(forecast, aes(x = date,
                          y = value,
                          color = variable)) +
  geom_line() +
  geom_hline(yintercept = 0,
             size = 0.25,
             linetype = 'solid',
             color = 'gray') +
  labs(title = 'HML excess return, j=12',
       y = 'Percent') +
  scale_color_viridis(discrete = TRUE,
                      option = 'D',
                      labels = c('Actual',
                                 'Benchmark AR', 
                                 'Benchmark Crowd',
                                 'Combined')) +
  theme(plot.title = element_text(hjust = 0.5),
        axis.title.x = element_blank(),
        legend.title = element_blank())

print(fi)

ggsave(filename = 'C://Users/cindy.lu/Downloads/forecastpower_HMLj12.png',
       plot = fi,
       width = 8,
       height = 6)