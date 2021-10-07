# EN: Used for cleaning original customers file
# ES: Utilizado para limpiar los clientes del archivo Datos_Clientes.csv
cat ../../2020/Datos_Clientes.csv | \
awk -F, '
BEGIN {
    print "customer_id,tension,tariff,power,department,section,segment,zone"
} {
    if ((NR>1) && ($5<40000)) print $1","$2","$3","$5","$9","$10","$11","$13
}' > customers.csv

# -> El primer bloque agrega los headers
# -> El segundo bloque saltea la primer linea (headers antiguos),
#    filtra por power menor a 40k y
#    imprime las columnas seleccionadas

# ---------------------------------------------------------

# EN: Used for changing the header of the original file Relacion_Cliente_Medidor.csv
# ES: Para cambiar el header de Relacion_Cliente_Medidor.csv

cat ../../2020/Relacion_Cliente_Medidor.csv | \
awk 'BEGIN {
    print "customer_id,meter_id"
} {
    if (NR>1) print $0
}' > rel_customer_meter.csv

# ---------------------------------------------------------

# EN: Filter the rel_customer_timer according to information of customers
# ES: Filtro de rel_customer_timer segun datos en customers

awk -F, '
BEGIN{
    print "id,id_nodo,fecha_desde,fecha_hasta" > "rel_NOcustomer_timer_filtered.csv";
    print "id,id_nodo,fecha_desde,fecha_hasta" > "rel_customer_timer_filtered.csv";
}
FNR==NR{cust[$1+0]; next}
{
    if($1+0 in cust){
        print $0 > "rel_customer_timer_filtered.csv"
    }else{
        print $0 > "rel_NOcustomer_timer_filtered.csv"
    }
}' \
<(tail -n+2 customers.csv) \
<(tail -n+2 Relacion_Cliente_Timer.csv)

# ---------------------------------------------------------

# EN: unique customers ids with consumption

tail -n+2 consumption_data_customers.csv | awk -F, '{print $2}' | sort | uniq > customers_with_consumption.csv

# ---------------------------------------------------------

# EN: remove lines whose id is not included in another file

head -n1 rel_customer_timer_filtered.csv > rel_customer_timer_filtered-2.csv
awk -F, '
FNR==NR{cust[$1+0]; next}
{
    if($1+0 in cust){
        print $0 > "rel_customer_timer_filtered-2.csv"
    }else{
        print $0 > "rel_NOcustomer_timer_filtered.csv"
    }
}
' \
<(cat customers_with_consumption.csv) \
<(tail -n+2 rel_customer_timer_filtered.csv)

# ---------------------------------------------------------

# EN: Filter lines in rel_customer_meter.csv according to information of customers
# ES: Filtro de rel_customer_meter.csv segun datos en customers

awk -F, '
BEGIN{
    print "customer_id,meter_id" > "rel_NO_customer_meter_filtered.csv";
    print "customer_id,meter_id" > "rel_customer_meter_filtered.csv";
}
FNR==NR{cust[$1+0]; next}
{
    if($1+0 in cust){
        print $0 > "rel_customer_meter_filtered.csv"
    }else{
        print $0 > "rel_NO_customer_meter_filtered.csv"
    }
}' \
<(tail -n+2 customers.csv) \
<(tail -n+2 rel_customer_meter.csv)

# ---------------------------------------------------------

# EN: Calculates how many electricwaterheater consumptions have total consumption

SSD_TMP_STORAGE="/scratch/$USER/$SLURM_JOBID"
mkdir -p $SSD_TMP_STORAGE
for bigfile in consumption_customers_*; do
    fname=$(echo $bigfile | awk -F/ '{print $(NF)}')
    cp -v $bigfile $SSD_TMP_STORAGE/
    TMP_CONSUM_FILE=splitted_$(echo $fname | cut -d. -f1)_$$
    split -n l/256 $SSD_TMP_STORAGE/$fname $SSD_TMP_STORAGE/$TMP_CONSUM_FILE$$
    FIRST_SPLIT=$(ls -lr $SSD_TMP_STORAGE/$TMP_CONSUM_FILE$$* | awk 'END{print $9}')
    sed -i '1d' $FIRST_SPLIT
    rm -rf $SSD_FILE_CONSUMPTION
    for file in $SSD_TMP_STORAGE/$TMP_CONSUM_FILE$$*; do
        awk -F, '
            {print $2}
        ' $file | sort | uniq \
        > $(echo $file | awk -F[_.] '{print $2"_"$3}')"_customers_with_consumption.csv" &
    done
done
wait
rm -fr $SSD_TMP_STORAGE/splitted_*
cat $SSD_FILE_CONSUMPTION/*_customers_with_consumption.csv | sort | uniq > customers_with_consumption.csv
rm -fr $SSD_TMP_STORAGE

# ---------------------------------------------------------

awk -F, '
FNR==NR{cust[$1+0]; next}
{
    if($1+0 in cust){
        IN+=1
    }else{
        OUT+=1
    }
}
END{print "IN: "IN"\nOUT: "OUT}
' \
<(cat THC/customers_with_consumption.csv) \
<(cat EWH/customers_with_consumption.csv)

# ------------------------------------------------------

# EN: Remove customer-meter relations without consumptions

awk -F, '
FNR==NR{cust[$1+0]; next}
{
    if($1+0 in cust){
        print $0 > "rel_customer_meter_filtered-2.csv"
    }else{
        OUT+=1
    }
}
' \
<(cat THC/customers_with_consumption.csv) \
<(cat THC/rel_customer_meter_filtered.csv)


# ------------------------------------------------------

# EN: Total customers ids (according to electricwaterheater and totalhousehold consuimption)

awk -F, '
FNR==NR{cust[$1+0]; next}
{
    if($1+0 in cust){
        print $0 > "customers-total-filtered.csv"
    }
}
' \
<(cat THC/customers_with_consumption.csv EWH/customers_with_consumption.csv | sort -n | uniq) \
<(cat THC/customers.csv)


# ------------------------------------------------------

# EN: Filter the total consumption of the electricwaterheater customers

awk -F, '
FNR==NR{cust[$1+0]; next}
{
    if($1+0 in cust){
        print $0 > "total_consumption_data.csv"
    }
}
' \
<(cat EWH/customers_with_consumption.csv) \
<(cat THC/consumption_data_201907.csv THC/consumption_data_201908.csv THC/consumption_data_201909.csv THC/consumption_data_201910.csv THC/consumption_data_201911.csv)

# ------------------------------------------------------

# Improvement from the previous

CONSUM_FILES[0]=THC/consumption_data_201907.csv
CONSUM_FILES[1]=THC/consumption_data_201908.csv
CONSUM_FILES[2]=THC/consumption_data_201909.csv
CONSUM_FILES[3]=THC/consumption_data_201910.csv
CONSUM_FILES[4]=THC/consumption_data_201911.csv

CUSTOMER_FILE=EWH/customers_with_consumption.csv

for file in "${CONSUM_FILES[@]}"; do
    fname=$(echo $file | awk -F/ '{print $(NF)}')
    echo $fname
    awk -F, '
    FNR==NR{cust[$1+0]; next}
    {
        if($2+0 in cust){
            print $0
        }
    }
    ' $CUSTOMERS_FILE $file  > "total_consumption_data_"$fname &
done
wait

cat total_consumption_data_*.csv > total_consumption_data.csv

# ------------------------------------------------------

# EN: Randomly select customers and filter their consumptions

sort -R customers_with_consumption.csv | head -n1000 > ./REDUCED/customers_ids.csv
CUST_IDS=$(cat ./REDUCED/customers_ids.csv | tr '\n' ',' | sed 's/,$/\n/')
for file in consumption_data_*; do
    fname=$(echo $file | awk -F/ '{print $(NF)}')
    echo $fname
    awk -F, -v CUST_IDS=$CUST_IDS '
    BEGIN{split(CUST_IDS,customers_ids,","); for(i in customers_ids){cust_ids[customers_ids[i]]}}
    {
        if($2+0 in cust_ids){
            print $0
        }
    }
    ' $file  > "./REDUCED/"$fname &
done
wait

# ------------------------------------------------------

# EN: Uniq to each file

for file in consumption_data_*; do
    fname=$(echo $file | awk -F/ '{print $(NF)}')
    echo $fname
    uniq $file | sort | uniq > "UNIQ_"$fname
done
wait

for file in UNIQ_consumption_data_*; do
    fname=$(echo $file | awk -F/ '{print $(NF)}')
    mv $file  $(echo $file | cut -b 6-)
done

# ------------------------------------------------------

# EN: Remove consumption of customers with less than 1440 (one day) records from EWH data

IDS=$(awk '{if(($1+0) < 1440){print $2} }' ./customers_numrecords.txt )
for c_id in $IDS; do
    sed -i '/,'$c_id',/d' consumption_data_customers_filtered.csv;
done

IDS=$(awk '{if(($1+0) < 1440){print $2} }' ./timers_numrecords.txt )
for c_id in $IDS; do
    sed -i '/,'$c_id',/d' consumption_data_timers_filtered.csv;
done

IDS_KEEP=$(awk '{if(($1+0) >= 1440){print $2} }' ./timers_numrecords.txt | tr '\n' ',' | sed 's/,$/\n/')
awk -F, -v CUST_IDS=$IDS_KEEP '
BEGIN{split(CUST_IDS,customers_ids,","); for(i in customers_ids){cust_ids[customers_ids[i]]}}
{
    if($2+0 in cust_ids){
        print $0
    }
}
' consumption_data_timers.csv  > consumption_data_timers_filtered.csv &
