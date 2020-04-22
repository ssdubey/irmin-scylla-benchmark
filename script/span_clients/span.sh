while getopts n:h:c:i:s: option   #n=#clinets h=list of hosts c=code file i=input file s=start
do 
 case "${option}" 
 in 
 n) no_of_clients=${OPTARG};; 
 h) hosts=${OPTARG};; 
 c) code_file=${OPTARG};; #name of the file to run. Position should be relative to a common folder. file should end with .exe
 i) inpfile_path=${OPTARG};;
 s) start=${OPTARG};;
 esac 
done 

#echo $no_of_clients
#template: cmd="dune exec set_span/${code_file} ${hosts} ${inpfile_path} --root=." 

cmd="dune exec set_span/${code_file} ${hosts} ${inpfile_path} --root=." 

for (( c=1; c<=${no_of_clients}; c++ ))
do  
   echo $c
   $cmd&
done
$cmd

#$cmd
#echo $cmd
#echo $code_file


#./span.sh -c set.exe -n 2 -i "/home/shashank/work/benchmark_irminscylla/input/hashing_overhead/1mb/kv" -h "172.17.0.2,172.17.0.3"
