# utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python
Transpose pivot wide using sql partitioning in wps r python
    %let pgm=utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python;

    Transpose pivot wide using sql partitioning in wps r python

    I would not use SQL to transpose this data structure, however SQL is capable.

    github
    https://tinyurl.com/3mvs6h7y
    https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python

    Original Post
    https://stackoverflow.com/questions/77000225/custom-reshaping-a-dataframe-in-r

    I have renamed the variables to match a previous R post, exactly the same problem.

    PROLEM

    /**************************************************************************************************************************/
    /*                       |                          |                                                                     */
    /*  INPUT                |                          |  OTPUT                                                              */
    /*                       |                          |                                                                     */
    /*  SD1.HAVE total obs=5 | 1 create SQL partitions  |  2 uses PARTITION                                                   */
    /*                       |                          |  for column names                                                   */
    /*                       |                          |                                                                     */
    /*   NAM    SCORE        | NAM  SCORE  PARTITION    |  NAM score1 score2 score3                                           */
    /*                       |                          |                                                                     */
    /*    A      0.30        |  A   0.30      1         |   A   0.30   0.45     NA                                            */
    /*    A      0.45        |  A   0.45      2         |   B   0.34  12.00     67                                            */
    /*    B      0.34        |  B   0.34      1         |                                                                     */
    /*    B     12.00        |  B  12.00      2         |                                                                     */
    /*    B     67.00        |  B  67.00      3         |                                                                     */
    /*                       |                          |                                                                     */
    /**************************************************************************************************************************/

    NOTES

      A good question like this, is worth a thousand answers.

      Here are the reasons SQL ay not be the best choice for this problem

        1. There is no sense of order or sequence in SQL. Partitioning is weak solution.

        2. Note if you change the order in the input the output changes

              NAM    SCORE      NAM    SCORE

               A      0.30  \    A      0.45    Was SCORE2 now SCORE1
               A      0.45  /    A      0.30    Was SCORE1
               B      0.34       B      0.34
               B     12.00       B     12.00
               B     67.00       B     67.00


        2. SQL most often expects a prmary key using dimension variables, not fact variables.
           SCORE is a fact variable. We need at least two dimesion variables.

        3. Often it is best to keep the long and skinny normalized form. It is often faster
           and more flexible.
           Suppose you want the max and min by score by nam.

        4. Codd and Date knew, when they created SQL that it would have limitations. when compared toa procedural languages
           like WPS, SAS, R and Python? I hppened to work at IBM when Codd was ignored.


     SOLUTIONS

         1 WPS proc R (hardcoded patitioning)
         2 WPS proc R (dynamic patitioning)
         3 WPS Python (dynamic patitioning)
         4 R no sql
           solution by user:13460602
         5 WPS no SQL
         6 WPS SQL hardcode
         7 WPS SQL dynamic
    /*                  _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */
    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.have;
      input  nam $1. score;
    cards4;
    A 0.30
    A 0.45
    B 0.34
    B 12.00
    B 67.00
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                       |                          |                                                                     */
    /*  INPUT                |                          |  OTPUT                                                              */
    /*                       |                          |                                                                     */
    /*  SD1.HAVE total obs=5 | 1 create SQL partitions  |  2 uses PARTITION                                                   */
    /*                       |                          |  for column names                                                   */
    /*                       |                          |                                                                     */
    /*   NAM    SCORE        | NAM  SCORE  PARTITION    |  NAM score1 score2 score3                                           */
    /*                       |                          |                                                                     */
    /*    A      0.30        |  A   0.30      1         |   A   0.30   0.45     NA                                            */
    /*    A      0.45        |  A   0.45      2         |   B   0.34  12.00     67                                            */
    /*    B      0.34        |  B   0.34      1         |                                                                     */
    /*    B     12.00        |  B  12.00      2         |                                                                     */
    /*    B     67.00        |  B  67.00      3         |                                                                     */
    /*                       |                          |                                                                     */
    /**************************************************************************************************************************/

    /*
     _                               _                   _               _
    / | __      ___ __  ___   _ __  | |__   __ _ _ __ __| | ___ ___   __| | ___
    | | \ \ /\ / / `_ \/ __| | `__| | `_ \ / _` | `__/ _` |/ __/ _ \ / _` |/ _ \
    | |  \ V  V /| |_) \__ \ | |    | | | | (_| | | | (_| | (_| (_) | (_| |  __/
    |_|   \_/\_/ | .__/|___/ |_|    |_| |_|\__,_|_|  \__,_|\___\___/ \__,_|\___|
                 |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.have r=have;
    submit;
    library(sqldf);
    want<-sqldf("
       select
          nam
         ,max(case when partition=1 then score else NULL end) as score1
         ,max(case when partition=2 then score else NULL end) as score2
         ,max(case when partition=3 then score else NULL end) as score3
       from
         ( select nam, score, row_number() OVER (PARTITION BY nam) as partition from have )
       group
         by nam;
       ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    ');

    The hardecoding is eliminates if we use SQL macro arrays.

    /*___                                    _                             _
    |___ \  __      ___ __  ___   _ __    __| |_   _ _ __   __ _ _ __ ___ (_) ___
      __) | \ \ /\ / / `_ \/ __| | `__|  / _` | | | | `_ \ / _` | `_ ` _ \| |/ __|
     / __/   \ V  V /| |_) \__ \ | |    | (_| | |_| | | | | (_| | | | | | | | (__
    |_____|   \_/\_/ | .__/|___/ |_|     \__,_|\__, |_| |_|\__,_|_| |_| |_|_|\___|
                     |_|                       |___/
    */

    /*---- This solution uses the concept od sql arrays                      ----*/
    /*---- It generates code for each partition                              ----*/
    /*---- In some case this can be very fast because compilers love         ----*/
    /*---- repetitive code, and my system have 32 logical processors         ----*/
    /*---- probably not well suited for high cardinality groupings           ----*/

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x("
    options validvarname=any;
    libname sd1 'd:/sd1';
    proc sql;select max(cnt) into :_cnt from (select count(nam) as cnt from sd1.have group by nam);quit;
    %array(_unq,values=1-&_cnt);
    proc r;
    export data=sd1.have r=have;
    submit;
    library(sqldf);
    want<-sqldf('
       select
          nam
         ,%do_over(_unq,phrase=%str(max(case when partition=? then score else NULL end) as score?),between=comma)
       from
          (select nam, score, row_number() OVER (PARTITION BY nam) as partition from have )
       group
         by nam
    ');
    want;
    endsubmit;
    run;quit;
    ");

    /*---- WHAT IS GOING ON                                                  ----*/

    proc sql;select max(cnt) into :_cnt from (select count(nam) as cnt from sd1.have group by nam);quit;
    %array(_unq,values=1-&_cnt);

    /*---- Max partition for column names                                    ----*/

    %put &=_unq1 ; /*  _UNQ1=1                                               ----*/
    %put &=_unq2 ; /*  _UNQ2=2                                               ----*/
    %put &=_unq3 ; /*  _UNQ3=3                                               ----*/

    %put &=_unqn ; /*  _UNQN=3                                               ----*/

    In this case this code produces two meta phrases using ate macro array above

    _cnt1 with the value 3 which is the group with the maximum count of records (B in this case)
    _cntn with the value 1 which means we are using just one group

    %do_over(_unq,phrase=%str(max(case when partition=? then score else NULL end) as score?),between=comma)

    This code generates. Which  I acall meta phrases,

       max(case when partition=1 then score else NULL end) as score1
      ,max(case when partition=2 then score else NULL end) as score2
      ,max(case when partition=3 then score else NULL end) as score3

    /*----  END COMMENTS                                                     ----*/

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS System                                                                                                        */
    /*                                                                                                                        */
    /*   nam score1 score2 score3                                                                                             */
    /*     A   0.30   0.45     NA                                                                                             */
    /*     B   0.34  12.00     67                                                                                             */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____                                    _   _                      _                             _
    |___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    __| |_   _ _ __   __ _ _ __ ___ (_) ___
      |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / _` | | | | `_ \ / _` | `_ ` _ \| |/ __|
     ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | || (_| | |_| | | | | (_| | | | | | | | (__
    |____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| \__,_|\__, |_| |_|\__,_|_| |_| |_|_|\___|
                     |_|         |_|                                          |_}

    */

    %utl_submit_wps64x("
    options validvarname=any lrecl=32756;
    libname sd1 'd:/sd1';
    proc sql;select max(cnt) into :_cnt from (select count(nam) as cnt from sd1.have group by nam);quit;
    %array(_unq,values=1-&_cnt);
    proc python;
    export data=sd1.have python=have;
    submit;
    print(have);
    from os import path;
    import pandas as pd;
    import numpy as np;
    import pandas as pd;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    want = pdsql('''
       select
          nam
         ,%do_over(_unq,phrase=%str(max(case when partition=? then score else NULL end) as score?),between=comma)
       from
          (select nam, score, row_number() OVER (PARTITION BY nam) as partition from have )
       group
         by nam
    ''');
    print(want);
    endsubmit;
    run;quit;
    "));

    /*           _               _
      ___  _   _| |_ _ __  _   _| |_
     / _ \| | | | __| `_ \| | | | __|
    | (_) | |_| | |_| |_) | |_| | |_
     \___/ \__,_|\__| .__/ \__,_|\__|
                    |_|

    */

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The PYTHON Procedure                                                                                                  */
    /*                                                                                                                        */
    /*    NAM  SCORE                                                                                                          */
    /*  0   A   0.30                                                                                                          */
    /*  1   A   0.45                                                                                                          */
    /*  2   B   0.34                                                                                                          */
    /*  3   B  12.00                                                                                                          */
    /*  4   B  67.00                                                                                                          */
    /*                                                                                                                        */
    /*    nam  score1  score2  score3                                                                                         */
    /*  0   A    0.30    0.45     NaN                                                                                         */
    /*  1   B    0.34   12.00    67.0                                                                                         */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*  _                                    _
    | || |    _ __   _ __   ___    ___  __ _| |
    | || |_  | `__| | `_ \ / _ \  / __|/ _` | |
    |__   _| | |    | | | | (_) | \__ \ (_| | |
       |_|   |_|    |_| |_|\___/  |___/\__, |_|
                                          |_|
    */

    libname sd1 "d:/sd1";

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.have r=have;
    submit;
    library(tidyverse);
    want<- data.frame();
    want <- have %>%
      mutate(rown = row_number(), .by=NAM ) %>%
      pivot_wider(names_from ="rown", values_from = "SCORE");
    colnames(want)<-paste0("SCORE",colnames(want));
    want;
    endsubmit;
    import data=sd1.want r=want;
    endsubmit;
    ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System Proc R                                                                                                  */
    /*                                                                                                                        */
    /* # A tibble: 2 x 4                                                                                                      */
    /*   SCORENAM SCORE1 SCORE2 SCORE3                                                                                        */
    /*   <chr>     <dbl>  <dbl>  <dbl>                                                                                        */
    /* 1 A          0.3    0.45     NA                                                                                        */
    /* 2 B          0.34  12        67                                                                                        */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* WPS                                                                                                                    */
    /*                                                                                                                        */
    /* Obs    SCORENAM    SCORE1    SCORE2    SCORE3                                                                          */
    /*                                                                                                                        */
    /*  1        A         0.30       0.45       .                                                                            */
    /*  2        B         0.34      12.00      67                                                                            */
    /*                                                                                                                        */
    /**************************************************************************************************************************/


    /*___   __        ______  ____                  ____   ___  _
    | ___|  \ \      / /  _ \/ ___|   _ __   ___   / ___| / _ \| |
    |___ \   \ \ /\ / /| |_) \___ \  | `_ \ / _ \  \___ \| | | | |
     ___) |   \ V  V / |  __/ ___) | | | | | (_) |  ___) | |_| | |___
    |____/     \_/\_/  |_|   |____/  |_| |_|\___/  |____/ \__\_\_____|

    */


    libname sd1 "d:/sd1";

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc transpose data=sd1.have out=sd1.want prefix=score;
     by nam;
     var score;
    run;quit;
    ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  WPS                                                                                                                   */
    /*                                                                                                                        */
    /*  Obs    NAM    _NAME_    SCORE1    SCORE2    SCORE3                                                                    */
    /*                                                                                                                        */
    /*   1      A     SCORE      0.30       0.45       .                                                                      */
    /*   2      B     SCORE      0.34      12.00      67                                                                      */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*__                                   _   _                   _               _
     / /_  __      ___ __  ___   ___  __ _| | | |__   __ _ _ __ __| | ___ ___   __| | ___
    | `_ \ \ \ /\ / / `_ \/ __| / __|/ _` | | | `_ \ / _` | `__/ _` |/ __/ _ \ / _` |/ _ \
    | (_) | \ V  V /| |_) \__ \ \__ \ (_| | | | | | | (_| | | | (_| | (_| (_) | (_| |  __/
     \___/   \_/\_/ | .__/|___/ |___/\__, |_| |_| |_|\__,_|_|  \__,_|\___\___/ \__,_|\___|
                    |_|                 |_|
    */

    libname sd1 "d:/sd1";

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    options validvarname=any;
    libname sd1 "d:/sd1";
    proc sql;
        create
          table sd1.want as
        select
          nam
          ,max(case when partition=1 then score else . end) as score1
          ,max(case when partition=2 then score else . end) as score2
          ,max(case when partition=3 then score else . end) as score3
       from (
             select monotonic() as partition , nam, score from sd1.have where nam="A" union
             select monotonic() as partition , nam, score from sd1.have where nam="B"
            )
       group
            by nam
    ;quit;
    ');

    proc print data=sd1.want;
    run;quit;

    /*____                                  _       _                             _
    |___  | __      ___ __  ___   ___  __ _| |   __| |_   _ _ __   __ _ _ __ ___ (_) ___
       / /  \ \ /\ / / `_ \/ __| / __|/ _` | |  / _` | | | | `_ \ / _` | `_ ` _ \| |/ __|
      / /    \ V  V /| |_) \__ \ \__ \ (_| | | | (_| | |_| | | | | (_| | | | | | | | (__
     /_/      \_/\_/ | .__/|___/ |___/\__, |_|  \__,_|\__, |_| |_|\__,_|_| |_| |_|_|\___|
                     |_|                 |_|          |___/
    */

    /*---- SETUP FOR SQL ARRAYS                                              ----*/

    /*---- Partitions max is 3  columns=SCORE1 SCORE2 SCORWE3                ----*/
    proc sql;select max(cnt) into :_scores from (select count(nam) as cnt from sd1.have group by nam);quit;
    %array(_score,values=1-&_cnt);

    %put &=_score1; /*  _SCORE1=1                                            ----*/
    %put &=_score2; /*  _SCORE2=2                                            ----*/
    %put &=_score3; /*  _SCORE3=3                                            ----*/

    %put &=_scoren; /*  _SCOREN=3                                            ----*/

    /*---- name array                                                        ----*/
    proc sql;select distinct(nam) into :_name1- from sd1.have;quit;
    %let _namen=&sqlobs;

    %put &=_name1;  /*  _NAME1=A                                             ----*/
    %put &=_name2;  /*  _NAME2=B                                             ----*/

    %put &=_namen;  /*  _NAMEN=2                                             ----*/

    /*---- END SETUP                                                         ----*/


    %utl_submit_wps64x(resolve('
    libname sd1 "d:/sd1";
    options validvarname=any;
    proc sql;
        create table want as select nam
            %do_over(_score,phrase=%str(
              ,max(case when partition=? then score else . end) as score?))
        from
            (%do_over(_name,phrase=%str(
              select monotonic() as partition , nam, score from sd1.have where nam="?"), between=union ))
        group by nam
    ;quit;

    proc print data=sd1.want;
    run;quit;
    '));

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  WPS                                                                                                                   */
    /*                                                                                                                        */
    /*  Obs    NAM    SCORE1    SCORE2    SCORE3                                                                              */
    /*                                                                                                                        */
    /*   1      A      0.30       0.45       .                                                                                */
    /*   2      B      0.34      12.00      67                                                                                */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
