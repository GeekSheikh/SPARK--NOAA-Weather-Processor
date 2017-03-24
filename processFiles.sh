echo "Process started at " `date +"%Y-%m-%d %H:%M:%S"`
basedir=/root/dataTmp/ftp.ncdc.noaa.gov/pub/data/noaa
workdir=${basedir}/tmp

if [ ! -d "${workdir}" ]; then
	mkdir -p ${workdir}
fi

for year in 2015 2016 2017
do
	echo "Staring ${year} at: " `date +"%Y-%m-%d %H:%M:%S"`
	echo "Starting process for year ${year}"
	echo "Copying data to working directory"
	cp -r ${basedir}/${year} ${workdir}
	indir=${workdir}/${year}

	outdir=${basedir}/output/${year}
	finaldir=${basedir}/output/master_${year}

	if [ ! -d "${outdir}" ]; then
	  mkdir -p ${outdir}
	fi

	if [ ! -d "${finaldir}" ]; then
	  mkdir -p ${finaldir}
	fi

	filenum=0
	totFileCount=`ls ${indir} | wc -l`
	echo "Total files to process for year ${year}: ${totFileCount}"
	echo "Starting Processing for ${year}"
	for file in ${indir}/*
	do
		let "filenum+=1"
		if [ $(( filenum % 25 )) == 0 ]; then
			echo "${filenum} of ${totFileCount} completed for ${year} at " `date +"%Y-%m-%d %H:%M:%S"`
		fi
		filename=$(basename "$file")
		extension="${filename##*.}"
		filename_short="${filename%.*}"
		gunzip $file
		$JAVA_HOME/bin/java -classpath ${basedir}/bin ishJava ${indir}/$filename_short ${outdir}/${filename_short}_processed
		cat ${outdir}/${filename_short}_processed >> ${finaldir}/${year}_master_processed
		rm -f ${outdir}/${filename_short}_processed
	done
	echo "Complete ${year} at " `date +"%Y-%m-%d %H:%M:%S"`
	echo "Block zipping processed data"
	pbzip2 ${finaldir}/${year}_master_processed
	echo "Finished Compressing at " `date +"%Y-%m-%d %H:%M:%S"`
	echo "Cleaning up for ${year}"
	rm -rf ${workdir}
done
echo "Complete at " `date +"%Y-%m-%d %H:%M:%S"`
