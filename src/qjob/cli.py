#! /usr/bin/env python
__author__  = "Marco Mariotti"
__email__   = "marco.mariotti@ub.edu"
__version__ = "0.0.3"
import sys, os, subprocess, shlex, shutil
from more_itertools import divide, chunked
from easyterm import command_line_options, read_config_file, write, printerr, check_file_presence, NoTracebackError


#### default options:
def_opt= {'i':'',           'c':'',        'd':'',
          'sys':'sge',
          'arr':'',      
          'n':'',           'o':'',
          'nlines':1,       'njobs':0,
          'q':'default_q',  'p':1,
          'm':'12',         't':'6',
          'setup':False,
          'head':'',        'foot':'',
          'e':False,        'so':'',
          'f':False,
          'bin':'~/bin',    'so':'',
          'E':'a',          'email':'youremail@domain.com',
          'joe':False,      'sl':False,
          'qsyn':'S=queue1,queue2;L=queue3,queue2',           
          'srun':False,
          'qsub':False,
          'pe':'smp',       'peq':'queue_arg1=pe_type1;queue_arg2=pe_type2' }

#### templates:
sge_header_template="""#!/bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -M {email} {queue_line}{time_line}{additional_options}
#$ -N {name}
#$ -l virtual_free={mem}G {cpus} 
"""
sge_header_single_job=sge_header_template+"""#$ -e {logerr}
#$ -o {logout}
"""
sge_header_array_job=sge_header_template+"""#$ -e {logerr}
#$ -o {logout}
#$ -t {range_str}
"""

sge_pe_template=  "\n#$ -pe {pe} {procs}"   ## for n of processors
slurm_pe_template="\n#SBATCH -c {procs}"   

slurm_header_template="""#!/bin/bash       
#SBATCH -J {name} {queue_line}{time_line}{additional_options}{cpus}
#SBATCH --mail-user={email}
#SBATCH --mem={mem}G
"""

slurm_header_single_job=slurm_header_template+"""#SBATCH -e {logerr}
#SBATCH -o {logout}
"""
slurm_header_array_job= slurm_header_template+"""#SBATCH -e {logerr}
#SBATCH -o {logout}
#SBATCH -a {range_str}
"""

#### help messages
help_msg="""qjob: utility to split commands into "jobs", then submit them to a queue of a SGE or Slurm cluster.

#### Minimal cluster_job usage:
   #1   qjob.py  -i input_lines.sh 
or #2   qjob.py  -c template_command.sh -d data_table.tsv

In #1 (direct mode), the user provide directly an input file containing commands. Each line must be 
 executable independently of others (i.e. each may become a single job submitted to the cluster).

In #2 (template mode), the user provides a template containing placeholders enclosed in {}, and a 
 data table (a tab-separated file including a header with column names). For each row, a command line
 is produced by replacing placeholders with data. Each must be executable independently of others.

By default, qjob creates an output folder in the current directory, named after the input.
It splits command lines in "jobs", and prepare files for their submission to the queue.

### Basic options:  (long and short names are equivalent)
-nlines | -nl   each submitted job will have X lines of commands (or X templates, in template mode)
-njobs  | -nj   set this to have a number of jobs X. Overrides -nlines
-qsub   | -Q    submit the jobs to the queue with qsub (SGE) or sbatch (Slurm)

### Job properties:
-q   queue name(s), comma separated; synonyms can be used (see -q_syn or run with -h default)
-m   GB of memory requested
-t   time limit requested in hours  (add m suffix for minutes, or d for days; e.g. -t 30m)
-p   number of processors requested (default: 1). Use -p 0 to not specify processors

## Other options:
-print_opt    prints default values for all options
-setup        use this before your first use; it creates a ~/.qjob configuration file
-h            print this help and exit. 
              Use "-h full" to inspect advanced options. 
              Documentation is available at https://qjob.readthedocs.io/ 
"""

long_help="""## Utilities:
-sys     cluster system; possible values: "sge" (default) or "slurm"
-o       output folder
-i       input file. Note: use "-" as argument to read standard input (if so you must provide -o)
-n       define base name of jobs (numerical suffixes will be added to each)
-joe     join std output and error logs; so that every job produce a single output file
-sl      use single log for all jobs, instead of 1 out, 1 err per job
-bin     in every job, the PATH variable is set to include this folder before any other
-head    file with header commands, included in each job before input commands
-foot    file with footer commands, included in each job after input commands
-e       do not export the current environment in the job 
-so      options for qsub (sge) or sbatch (slurm). Use quotes: e.g. -so " -tc 5 "
-arr     submit as array job. Provide an index range (e.g. 1-10) as argument 
         Requires a template input (-c) including taskid variables; -d may be omitted         
-f       force overwrite of jobs folder if existing. By default, qjob prompts the user
-qsyn    defining synonyms for -q, using format: "SYN_NAME=queue1;OTHER_SYN=queue2,queue3"
-email   email provided when submitting job
-E       send an email in conditions determined by the argument. Multiple ones can be concatenated, e.g. -E abe
    b:  at the beginning of the job;          e:  end of the job; 
    a:  if aborted (or rescheduled in sge);   s:  suspended <sge only>;   v:  verbose, mails for anything

## SGE system only:
-pe     +   parallelization environment for SGE (default: smp); taken from -peq if provided
-peq    +   defining which -pe to use based on the -q argument; format: "QUEUE_X:pe1;SYNONYM_Y:pe2"

## Slurm system only:
-srun       slurm only; prefix each command line by "srun "
"""

command_line_synonyms={'Q':'qsub', 'nj':'njobs', 'nl':'nlines', 'force':'f',
                       'tp':'pe'}

#########################################################
###### start main program function

def main(args={}):
  """Main function of the program, run when this is executed from the command line """

  # printing program startup header
  for i in range(16):
    write(' ', end='', how=['reverse', None][i%2])
  write('---<<<{:^20}>>>---'.format( f'qjob v{__version__}' ),  end='', how='reverse')
  for i in range(16):
    write(' ', end='', how=['reverse', None][(i+1)%2])
  write('')

  ## loading options  
  if not args:
    user_config_file=os.path.expanduser('~') + '/.qjob'

    ################### -setup
    # first run: writing ~/.qjob file
    if '-setup' in sys.argv:
      if os.path.isfile(user_config_file):
        raise NoTracebackError(f'qjob ERROR -setup was given but file {user_config_file} exists!\n'
                               f'If you really want to restore built-in defaults, delete the file and run again: qjob -setup')      
      write(f'\nSet up mode! Creating file {user_config_file}  ...' )
      ## reading any options that user may have specified to write them to default config
      opt=command_line_options(def_opt, help_msg,  'i',
                               synonyms=command_line_synonyms,
                               advanced_help_msg={'full':long_help} )
      with open(user_config_file, 'w') as fh:
        for key, value in opt.items():
          if key not in def_opt or key=='setup': continue # skipping special options such as -h
          if type(value) is list:            
            fh.write(f'{key}  = {" ".join(value)}\n')
          else:
            fh.write(f'{key}  = {value}\n')
      write(  f'\nDone! Now do the following:\n'
              f'1) Run qjob -h to inspect basic options, or qjob -h full for advanced options\n'
              f'2) You may edit {user_config_file} to set your default options (system, queue, job properties)\n'
              f'3) Try and submit a test job, e.g. a file containing "echo qjob is working ok"')
      raise NoTracebackError('')
    ################### -setup ### over
    
    # reading options from config file and command line
    if not os.path.isfile(user_config_file):
      raise NoTracebackError(f'qjob ERROR the file {user_config_file} is not found.\nFirst time user? Then run qjob -setup')
    else:
      conf_opt = read_config_file(user_config_file, types_from=def_opt)
      def_opt.update(conf_opt)
      opt=command_line_options(def_opt, help_msg,  'i',
                               synonyms=command_line_synonyms,
                               advanced_help_msg={'full':long_help} )

  ## checking input options      
  if not opt['sys'] in ['sge', 'slurm']:
    raise NoTracebackError('qjob ERROR -sys  must be one of  sge, slurm')
  
  if (  not opt['i'] and
       not ( (opt['arr'] and opt['c']) or     # array mode not active
             (opt['c']   and opt['d']))       #template mode not active
      or (opt['i'] and (opt['c'] or opt['d'] or opt['arr']))  ): # input active but others specified
    raise NoTracebackError(f'qjob ERROR you must provide either option -i, or options -c and -d, '
                           f'or options -c and -arr. \nRun qjob -h for more info')

  # checking -arr is in the right format, if specified
  if opt['arr']:
    try:
      a, b = opt['arr'].split('-')
      int(b.split(':')[0])
      int(a)
    except (ValueError, IndexError) as e:
      raise NoTracebackError(f'qjob ERROR option -arr must contain a valid array range specification, such as 1-100') from None
  
  ## reading input command lines
  if opt['i']:
    # direct input mode
    if opt['i']=='-':
      write('Input: stdin')
      if not opt['o']:
        raise NoTracebackError("qjob ERROR you must specify job name with -o if reading from standard input!")
      input_commands=[line.strip() for line in sys.stdin]
    else:
      write(f'Input: file {opt["i"]}')      
      check_file_presence(opt['i'], 'inputfile (option -i)')
      cmd_lines=[]
      with open(opt['i']) as tfh:
        for line in tfh:
          s=line.strip()
          if s and not s.startswith('#'):
            cmd_lines.append(s)
            
  else:
    # template input mode
    write(f'Input: template file {opt["c"]}')          
    check_file_presence(opt['c'], 'template command file (option -c)')
    template_line='\n'.join( [line.strip() for line in open(opt['c'])] )
    cmd_lines=[]
    if opt['d']:
      write(f'Input: data table {opt["d"]}')                
      check_file_presence(opt['d'], 'data file to fill template (option -d)')
      fh=open(opt['d'])
      fields=fh.readline().split('\t')
      for line_index, line in enumerate(open(opt['d'])):
        s=line[:-1].split('\t')
        if len(s)!=len(fields):
          raise NoTracebackError(f"qjob ERROR parsing data file -d {opt['d']}: line n.{line_index} has {len(s)} fields while header has {len(fields)}. Here's the line:\n{line[:-1]}")
        datarow={ f:s[fi]  for fi, f in enumerate(fields)}
        try: 
          this_command_line=template_line.format( **datarow )
        except err:
          printerr(f"qjob ERROR parsing data file -d {opt['d']} in line n.{line_index} when filling template with data:")
          raise err from None
        cmd_lines.append(this_command_line)
        
    elif opt['arr']: #array mode with data table
      cmd_lines=[template_line]
        
  ####  now cmd_lines is defined; each one can be executed independently of others
  # # debug
  # for i, cl in enumerate(cmd_lines):
  #   write(cl, how=['green', 'yellow'][i%2])

  ## reading custom synonyms
  queue_synonyms={}
  if opt['qsyn']:
    for assign_piece in opt['qsyn'].split(';'):
      syn_name, queue =assign_piece.split('=') #queue can be comma separated but we pass it as it is
      queue_synonyms[syn_name]=queue
  pe_table={}
  if opt['peq']:
    for assign_piece in opt['peq'].split(';'):
      queue, pe =assign_piece.split('=') 
      pe_table[queue]=pe

  ###  determining number of jobs, number of lines
  tot_lines=len(cmd_lines)
  if tot_lines==0:    raise NoTracebackError("qjob ERROR the list of command lines is empty!")
  # if tot_lines==1:
  #   array_mode=False
  #   # n_lines_per_job=1
  #   # n_jobs=1
  # elif opt['arr']:
  #   array_mode=True
  #   # n_lines_per_job=1 # actually not used; just a dev reminder
  #   # n_jobs=1          # same as above
  # else: #standard case
  #   array_mode=False    
  # #   if opt['njobs']:
  # #     n_lines_per_job= tot_lines  / opt['njobs']
  # #   else: 
  # #     n_lines_per_job= opt['nlines']
  # #   n_jobs=  (tot_lines-1) // n_lines_per_job +1

  # write(f' n_jobs = {n_jobs}   n_lines_per_job = {n_lines_per_job}')
    
  ### Deriving output folder
  if not opt['o']:
    if opt['i']:
      output_folder= opt['i']+'.jbs'
    else:
      output_folder= opt['c']+'.jbs'      
  else:
    output_folder= opt['o'].rstrip('/')+'.jbs'

  ## job name
  if opt['n']:
    prefix_name=opt['N'].rstrip('.')
  else:
    prefix_name=os.path.basename( output_folder[:-4] )

  ### output folder (and rewrite)
  if os.path.isdir(output_folder):
    if not opt['f']:
      if not ( input(f'Jobs folder {output_folder}/ existing from a previous run; overwrite?\n'
                   f'This will delete previous log files, if present.\nReply= [Y] ') 
                   in ['', 'Y', 'y', 'yes'] ):
        raise NoTracebackError("Aborted. ")
    shutil.rmtree(output_folder)
  os.mkdir(output_folder)

  ### header/footer commands
  init_command=''
  if opt['bin']:
    init_command += 'export PATH='+opt['bin']+':$PATH\n'
  if opt['head']:
    init_command += join([ line.strip() for line in open(opt['head']) ], '\n')  ##adding header lines
  footer_command=''  if not opt['foot'] else (
    join([ line.strip() for line in open(opt['foot']) ], '\n') )  ##adding footer lines

  ## determining queue
  queue_name=opt['q'] if not opt['q'] in queue_synonyms else queue_synonyms[opt['q']]
  time_limit_minutes=None
  if opt['t']:   
    if   str(opt['t']).endswith('m'):       time_limit_minutes=int(opt['t'][:-1])  
    elif str(opt['t']).endswith('d'):       time_limit_minutes=int(opt['t'][:-1])*60*24
    elif str(opt['t']).endswith('h'):       time_limit_minutes=int(opt['t'][:-1])*60
    else:                                   time_limit_minutes=int(opt['t'])*60

  ## remaining lines to be put in job
  additional_options=''
  if   opt['sys']=='sge':
    ## queue or partition
    queue_line= "\n#$ -q {}".format(queue_name) if queue_name else ''
    ## time constraint
    time_line='\n#$ -l h_rt={h}:{m}:00'.format(h=time_limit_minutes//60, m=time_limit_minutes%60)  if not time_limit_minutes is None else ''
    ## email
    if opt['E']:          additional_options+='\n#$ -m {} '.format(opt['E']  if not opt['E']=='v' else 'abes')
    ## environmental vars
    if not opt['e']:      additional_options+='\n#$ -V '
    ## cpus
    parallelization=opt['pe']    if not queue_name in pe_table else pe_table[queue_name]
    cpu_specs=sge_pe_template.format(procs=opt['p'], pe=parallelization)     if opt['p'] else ''
   
  elif opt['sys']=='slurm':
    ## queue or partition
    queue_line= "\n#SBATCH -p {}".format(queue_name) if queue_name else ''
    ## time constraint
    time_line=  '\n#SBATCH -t 0-{h}:{m}'.format(h=time_limit_minutes//60, m=time_limit_minutes%60)  if not time_limit_minutes is None else ''
    ## email
    if opt['E']:          
      sge_mail_codes2slurm_code={'a':'FAIL', 'b':'BEGIN', 'e':'END', 'v':'ALL'}
      for code in opt['E']: 
        if not code in sge_mail_codes2slurm_code: raise notracebackException("ERROR this -E option is not valid for slurm: {}".format(code))
        additional_options+='\n#SBATCH --mail-type={}'.format(sge_mail_codes2slurm_code[code])
    ## environmental vars
    if not opt['e']:        additional_options+='\n#SBATCH --export ALL'
    ## cpus
    cpu_specs=slurm_pe_template.format(procs=opt['p']) if opt['p'] else ''

  ## easy handled options:
  mem=opt['m']
  submit_add_options=opt['so']
  suffix_out='LOG'
  suffix_err='ERR' if not opt['joe'] else 'LOG'

  def write_job(cmd, name, outfile, output_folder):
    """ Takes the command, plus all other variables computed and available in namespace, prepares a single job file and submit it if necessary"""
    logout='{outfile}.{suf}'.format(outfile=outfile, suf=suffix_out)
    logerr='{outfile}.{suf}'.format(outfile=outfile, suf=suffix_err)
    if opt['sl']:
      logout='{outfolder}output_all_jobs.{suf}'.format(outfolder=output_folder, suf=suffix_out)
      logerr='{outfolder}output_all_jobs.{suf}'.format(outfolder=output_folder, suf=suffix_err)

    write('Writing file: '+outfile, end=' ')
    if   opt['sys']=='sge':
      header=sge_header_single_job.format(email=opt['email'],
                                          additional_options=additional_options,
                                          queue_line=queue_line, 
                                          time_line=time_line,
                                          name=name,
                                          outfile=outfile,
                                          cpus=cpu_specs,
                                          mem=mem,
                                          logout=logout,
                                          logerr=logerr)
    elif opt['sys']=='slurm':
      append_add='\n#SBATCH --open-mode=append'  if opt['sl'] else ''
      if opt['srun']: cmd='\n'.join( ['srun '+x for x in [i.strip() for i in cmd.split('\n') if i.strip()]] )
      header=slurm_header_single_job.format(email=opt['email'],
                                            additional_options=additional_options + append_add,
                                            queue_line=queue_line, 
                                            time_line=time_line,
                                            name=name,
                                            outfile=outfile,
                                            cpus=cpu_specs,
                                            mem=mem,
                                            logout=logout,
                                            logerr=logerr)

    with open(outfile, 'w') as ofh:
      ofh.write(header + init_command.rstrip('\n') + '\n' +
                cmd.rstrip('\n')+'\n'+
                footer_command)
      
    if opt['qsub']:
      write(' \tsubmitting file! ', end='')
      if   opt['sys']=='sge':
        subprocess.run( shlex.split(f'qsub {submit_add_options} {outfile}'),
                        capture_output=True, check=True)
        
      elif opt['sys']=='slurm':
        subprocess.run( shlex.split(f'sbatch {submit_add_options} {outfile}'),
                        capture_output=True, check=True)
    write('')

  def write_array_job(cmd, name, outfile, arr_range, output_folder):
    """ Takes the command list, plus all other variables computed and available in namespace, prepares an array file and submit it if necessary"""
    write(f'Writing array file : {outfile}', end=' ')
    if   opt['sys']=='sge':
      logout='{outfile}.$TASK_ID.{suf}'.format(outfile=outfile, suf=suffix_out)
      logerr='{outfile}.$TASK_ID.{suf}'.format(outfile=outfile, suf=suffix_err)
      if opt['sl']: 
        logout='{outfile}.{suf}'.format(outfile=outfile, suf=suffix_out)
        logerr='{outfile}.{suf}'.format(outfile=outfile, suf=suffix_err)

      header=sge_header_array_job.format(email=email,
                                         additional_options=additional_options,
                                         queue_line=queue_line, 
                                         time_line=time_line,
                                         name=name,
                                         outfile=outfile,
                                         cpus=cpu_specs,
                                         mem=mem,
                                         logout=logout,
                                         logerr=logerr,
                                         range_str=arr_range)

    elif opt['sys']=='slurm':
      logout='{outfile}.%a.LOG'.format(outfile=outfile)
      logerr='{outfile}.%a.ERR'.format(outfile=outfile)
      if opt['sl']: 
        logout='{outfile}.{suf}'.format(outfile=outfile, suf=suffix_out)
        logerr='{outfile}.{suf}'.format(outfile=outfile, suf=suffix_err)
      append_add='\n#SBATCH --open-mode=append'  if opt['sl'] else ''        

      header=slurm_header_array_job.format(email=email,
                                           additional_options=additional_options + append_add,
                                           queue_line=queue_line, 
                                           time_line=time_line,
                                           name=name,
                                           outfile=outfile,
                                           cpus=cpu_specs,
                                           mem=mem,
                                           logout=logout,
                                           logerr=logerr,
                                           range_str=arr_range)
      if opt['srun']: cmd='\n'.join( ['srun ' + i.strip()   for i in cmd.split('\n') if i.strip()] )

    with open(outfile, 'w') as ofh:
      ofh.write(header + init_command.rstrip('\n') + '\n' +
                cmd.rstrip('\n')+'\n'+
                footer_command)

    if opt['qsub']:
      write(' \tsubmitting file! ', end='')
      if   opt['sys']=='sge':
        subprocess.run( shlex.split(f'qsub {submit_add_options} {outfile}'),
                        capture_output=True, check=True)
        
      elif opt['sys']=='slurm':
        subprocess.run( shlex.split(f'sbatch {submit_add_options} {outfile}'),
                        capture_output=True, check=True)
    write('')

      
  ######## array mode
  if opt['arr'] and not tot_lines==1:
    name=prefix_name 
    outfile=os.path.abspath(output_folder+name)
    cmd='\n'.join(cmd_lines)   # array mode wants a single job submitted (with TASK_ID)    
    write_array_job(cmd, name, outfile, opt['arr'], output_folder)

  ####### normal mode
  else:    
    ## from here it goes only if we're not in array job mode
    # producing a "cmd" variable with all the lines to put in a job; then we write (and submit it)

    if opt['njobs']:
      cmd_iter= divide(opt['njobs'], cmd_lines)
    else:
      cmd_iter= chunked(cmd_lines, opt['nlines'])
    
    for job_index, job_commands in enumerate(cmd_iter, 1):
      cmd='\n'.join(job_commands)
      name=f'{prefix_name}.{job_index}'
      outfile=os.path.abspath(f'{output_folder}/{name}')
      write_job(cmd, name, outfile, output_folder) 

  write('\nqjob: all done, quitting')


#######################################################################################################################################
if __name__ == "__main__":
  main()
