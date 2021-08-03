options nosource nodate nocenter nonumber fullstimer ps=max ls=72;
%include '/wrds/lib/utility/wrdslib.sas';
options sasautos=('/wrds/wrdsmacros/', SASAUTOS) MAUTOSOURCE;

/********************************************************************************************************/
libname taq '/wrds/taq/sasdata'; * for 1993-2014;
libname taqmsect '/wrds/nyse/sasdata/taqms/ct'; * for after 2014;
libname taqmsecq '/wrds/nyse/sasdata/taqms/cq'; * for after 2014;
libname mlink '/wrds/wrdsapps/sasdata/linking/tclink' inencoding=asciiany; * links for after 2014;
libname mseclink '/wrds/wrdsapps/sasdata/linking/taqmclink' inencoding=asciiany; * links for after 2014;
%let start_time_m = '9:30:00't;    * starting time_m;
%let end_time_m = '10:05:00't;    * ending time_m;
%let interval_seconds = 300;    * interval is 15*60 seconds (15 minutes);
%let date_ymd_arg = 20170103;    * date to get data for;
%let output_file = '/home/duke/sa400/HFZoo/code/wrds_old/temp/test.csv'; * name of output file;
%let permno_list = (24942, 17830); * permnos to grab (in case not in master);
%let cusip_list = ('594918104', '30231G102', '369604103', '75513E101'); * cusips to grab (in case not in link);
%let symbol_list = ('SPY', 'AAPL'); * symbols (in case not in link);
%let date_ym_arg = 201301;    * date to get data for;
/********************************************************************************************************/


%macro VarExist(ds, var);
    %local rc dsid result;
    %let dsid = %sysfunc(open(&ds));
 
    %if %sysfunc(varnum(&dsid, &var)) > 0 %then %do;
        %let result = 1;
        %put NOTE: Var &var exists in &ds;
    %end;
    %else %do;
        %let result = 0;
        %put NOTE: Var &var not exists in &ds;
    %end;
 
    %let rc = %sysfunc(close(&dsid));
    &result
%mend VarExist;


/* Get TAQ master table */

data taq_master_table;
    set taqmsec.mastm_&date_ymd_arg;
    where (cusip in &cusip_list
    or symbol_15 in &symbol_list);
    rename cusip = cusip9 symbol_15 = symbol_taq;
    length cusip8_taq $ 8;
    cusip8_taq = left(cusip);
run;

data taq_master_table;
    set taq_master_table;
    if (%VarExist(taq_master_table, cta_symbol)) then do;
        sym_root = scan(symbol_taq, 1, ' ');
        sym_suffix = scan(symbol_taq, 2, ' ');
        end;
    else do;
        rename symbol_root = sym_root symbol_suffix = sym_suffix;
    end;
run;
    
    
/* Get TAQ CRSP link */

data crsp_taq_link_table;
    set mseclink.taqmclink;
    where (date = input(put(&date_ymd_arg,8.),yymmdd8.)
    and permno in &permno_list);   
    rename cusip=cusip8_crsp;
run;


/* Combine TAQ Master and CRSP Link to get all (permnos, cusips, symbols) */

proc sql; 
    create table crsp_taq_link_master as
    select a.permno,
        coalesce(a.cusip8_crsp, b.cusip8_taq) AS cusip8,
        coalesce(a.sym_root, b.sym_root) AS sym_root,
        coalesce(a.sym_suffix, b.sym_suffix) AS sym_suffix
        from crsp_taq_link_table as a 
        full join taq_master_table as b 
        on a.sym_root=b.sym_root and a.sym_suffix=b.sym_suffix;  
run;


/* Use CRSP price dataset to deal with missing permnos */

data crsp_prices;
    set crsp.dsf;
    where (date = input(put(&date_ymd_arg,8.),yymmdd8.)); 
    rename cusip=cusip_nonlink;
run;

proc sql; 
    create table crsp_taq_link_master_clean as
    select coalesce(a.permno, b.permno) AS permno, 
         a.cusip8, a.sym_root, a.sym_suffix,
         b.askhi, b.bidlo
        from crsp_taq_link_master as a 
        left join crsp_prices as b 
        on a.cusip8 = b.cusip_nonlink; 
run;


/* Use link table to grab TAQ prices */

proc sql;
    create table taq_trades_link as
    select a.*, 
            b.permno, b.sym_root, b.sym_suffix, b.cusip8, b.askhi, b.bidlo
      from taqmsect.ctm_&date_ymd_arg as a
      right join crsp_taq_link_master_clean as b
      on a.sym_root=b.sym_root and a.sym_suffix=b.sym_suffix
      where (
        time_m between &start_time_m and &end_time_m
        and (price > 0) and (size > 0)
        and (ex <> 'D')
        and (tr_scond in ('', '@', 'E', '@E', 'F', 'FI', '@F', '@FI', 'I', '@I'))
        and (tr_corr in ('00', '01'))
      );
quit;

data taq_trades_link;
    set taq_trades_link;
    length ticker_identifier $ 30;
    ticker_identifier = cats(permno, '_', cusip8, '_', sym_root, '.', sym_suffix);
run;

proc sort data=taq_trades_link out=taq_trades_link;
    by ticker_identifier ex;
quit;

/* Get highest vol exchanges for each symbol */

proc sql;
    create table exchange_sums as 
        select a.ticker_identifier, a.ex,
        sum(size) as ex_trading_vol_total
        from taq_trades_link as a 
        group by a.ticker_identifier, a.ex;
quit;

proc sort data=exchange_sums out=exchange_sums;
    by ticker_identifier ex_trading_vol_total;
quit;

data exchange_sums_max;
    set exchange_sums;
    by ticker_identifier;
    if (last.ticker_identifier);
run;


/* Merge with TAQ data and filter */
   
proc sql;
    create table taq_trades_link_filter as
    select a.*
      from taq_trades_link as a
      left join exchange_sums_max as b
      on a.ticker_identifier = b.ticker_identifier
      where (
        a.ex = b.ex
        and a.price <= coalesce(a.askhi, 99999)
        and a.price >= coalesce(a.bidlo, 0)
      );
quit;


/* Get last trade in sequence for those with same timestamps */

proc sql;
    create table taq_trades_link_filter_seq as
    select *
        from taq_trades_link_filter as a
    left join 
        (select  max(tr_seqnum) as max_sequence_number, time_m, ticker_identifier 
        from taq_trades_link_filter group by time_m, ticker_identifier)  as b
    on (a.time_m = b.time_m and a.ticker_identifier = b.ticker_identifier)
    where (tr_seqnum = max_sequence_number);
quit;


/* Moving average filter for each stock */
/* Trim observation i if |p_i -MM(p_i)| > eta*s_i(k) + gamma*/
/* where MM(p_i) is a centered, moving_median(k,k) not including the present price */
/* s_i(k) is similar but mean abs deviation, and eta and gamma are  */
/* arbitrary parameters */


proc sort data=taq_trades_link_filter_seq out=taq_trades_link_filter_seq_rev;
    by ticker_identifier time_m;
run;

proc expand data=taq_trades_link_filter_seq_rev out=taq_trades_link_filter_seq_rev method=none;
   id time_m;
   by ticker_identifier;
   convert price = price_MM / transout=(cmovmed 51);
run;

data taq_trades_link_filter_seq_rev;
   set taq_trades_link_filter_seq_rev;
   abs_dev = abs(price - price_MM);
run;

proc expand data=taq_trades_link_filter_seq_rev out=taq_trades_link_filter_seq_rev method=none;
   id time_m;
   by ticker_identifier;
   convert abs_dev = dev_MA_k1_c / transout=(cmovave 51);
run;

data taq_trades_link_filter_seq_rev;
    set taq_trades_link_filter_seq_rev;
    dev_MA_k_xc = (dev_MA_k1_c*51-abs_dev)/50;
    q4_filter_level = 10*dev_MA_k_xc + 0.01;
    q4_drop_indicator = (abs_dev > q4_filter_level);
run;

data taq_trades_link_filter_seq_rev;
    set taq_trades_link_filter_seq_rev;
    where (q4_drop_indicator = 0);
run;


/* Clean up and get price interval */

proc sort data=taq_trades_link_filter_seq_rev out=prices_clean_sort;
    by ticker_identifier time_m;
run;

data sample_prices;
    set prices_clean_sort;
    by ticker_identifier time_m;
    retain itime_m rtime_m iprice; *Carry time and price values forward;
       format itime_m time12. rtime_m time12.9; * if you only need second timestamp, use 'time12.' instead.;
    if first.ticker_identifier=1 then do;
       */Initialize time_m and price when new symbol or date starts;
        rtime_m=time_m;
        iprice=price;
        itime_m= &start_time_m;
    end;
    if time_m >= itime_m then do; /*Interval reached;*/
        output; /*rtime_m and iprice hold the last observation values;*/
        itime_m = itime_m + &interval_seconds;
        do while(time_m >= itime_m); /*need to fill in all time_m intervals;*/
            output;
            itime_m = itime_m + &interval_seconds;
        end;
    end;
rtime_m=time_m;
iprice=price;
keep ticker_identifier date itime_m iprice;
rename itime_m = time iprice = price;
run; 


/* Export to CSV */

%macro check(file);
%if %sysfunc(fileexist(&file)) ge 1 %then %do;
   %let rc=%sysfunc(filename(temp,&file));
   %let rc=%sysfunc(fdelete(&temp));
%end; 
%else %put The file &file does not exist;
%mend check; 

%check(&output_file);

proc export data=sample_prices
    outfile=&output_file
    dbms=dlm;  
    delimiter=',';
run;


quit;
