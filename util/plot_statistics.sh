#!/bin/bash

plot_data_series()
{
	local column_x=1
	local x_axis_title="Statistics"	
	local height=$((PLOTS * 3))
	
	# Generate gnuplot script
	local plot_script="set output \"${OUTPUT_FILE}\"
		set terminal pdf size $ELEMENTS,$height font \"Helvetica,6\"
		set multiplot layout $PLOTS, 1 scale 1, 1
		set offsets 0, 0, 0.2, 0
		set decimal \".\"
		set format y \"%'g\"
		set datafile separator \"\t\"
		set style data histogram
		set style fill solid border -1
		set key horiz center top outside
		set grid mytics
		set auto x
		set xlabel \"${x_axis_title}\"
		set xtics rotate by -90 nomirror right offset -6.5,1.0
		set logscale y 10
		set yrange [0.01:]
		set mytics 10
		set ylabel \"Time in µs\""
	
	if [ "$PERCENTILES" = true ]; then
		plot_script="$plot_script
			plot \"tmp_sorted.csv\" using (\$5/1000.0):xtic(1) title \"Average\", '' u (\$6/1000.0) ti \"Minimum\", '' u (\$7/1000.0) ti \"Maximum\", '' u (\$10/1000.0) ti \"95th\", '' u (\$11/1000.0) ti \"99th\", '' u (\$12/1000.0) ti \"99.9th\""
	else
		plot_script="$plot_script
			plot \"tmp_sorted.csv\" using (\$5/1000.0):xtic(1) title \"Average\", '' u (\$6/1000.0) ti \"Minimum\", '' u (\$7/1000.0) ti \"Maximum\""
	fi
	
	plot_script="$plot_script
		unset grid
		set xtics offset -2.0,1.0
		set yrange [1:]
		set ylabel \"Number of Operations\"
		plot \"tmp_sorted.csv\" using 3:xtic(1) notitle, \"\" using 0:3:3 with labels offset 2.5,0.5 notitle
		set ylabel \"Operations per Second\"
		plot \"tmp_sorted.csv\" using 4:xtic(1) notitle, \"\" using 0:4:4 with labels offset 2.5,0.5 notitle
		set ylabel \"Counter\""

	if [ "$COUNTER1" = true ]; then
		plot_script="$plot_script
			plot \"tmp_sorted.csv\" using 8:xtic(1) notitle, \"\" using 0:8:8 with labels offset 2.5,0.5 notitle"
	fi
	
	if [ "$COUNTER2" = true ]; then
		plot_script="$plot_script
			plot \"tmp_sorted.csv\" using 9:xtic(1) notitle, \"\" using 0:9:9 with labels offset 2.5,0.5 notitle"
	fi
	
	# Execute plot
	gnuplot <<- EOF
		$plot_script
	EOF
}

process_data()
{
	PERCENTILES=false
	COUNTER1=false
	COUNTER2=false
	PLOTS=3

	touch tmp.csv
	for file in $INPUT_FOLDER*.csv; do
		cat $file >> tmp.csv
		
		columns=`awk -F'[\t]+' '{print NF}' $file | head -n 1`
		if [ "$columns" != "" ] && [ "$columns" -gt 9 ]; then
			PERCENTILES=true
		fi
		
		while read line; do
			counter1=`echo "$line" | cut -d $'\t' -f 8`
			counter2=`echo "$line" | cut -d $'\t' -f 9`
			if [ "$counter1" != "0" ]; then
				if [ "$COUNTER1" = false ]; then
					PLOTS=$((PLOTS + 1))
				fi
				COUNTER1=true
			fi
			if [ "$counter2" != "0.0000" ]; then
				if [ "$COUNTER2" = false ]; then
					PLOTS=$((PLOTS + 1))
				fi
				COUNTER2=true
			fi
		done < $file
	done
	
	ELEMENTS=$((`cat tmp.csv | wc -l` / 2))
	
	sort tmp.csv > tmp_sorted.csv
}

cleanup()
{
	rm -f tmp.csv
	rm -f tmp_sorted.csv
}

WORKING_DIR=$(pwd)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" != 2 ]; then
	echo "Usage: $0 <input folder> <output file1>" >&2	
	exit -1
fi

INPUT_FOLDER="$1"
OUTPUT_FILE="$2"
PLOT_SCRIPT="/tmp/plot.gp"

process_data
plot_data_series
cleanup

echo "Finished. All output located in ${RESULTS_DIR}"
