Installation
------------

We recommend to use the conda package manager to install qjob
(check `this page to install conda <https://docs.conda.io/en/latest/miniconda.html>`_).
**Run this in a terminal to install qjob** in your conda environment::

    conda install -c mmariotti qjob

Now, before you start using qjob, run this command::

    qjob -setup

This will create a qjob configuration file in your home: ``~/.qjob``.
    
This file contains all qjob default options for your user.
Open the file in any text editor, and check (and modify if necessary) at least these items:

  - ``sys``: your queueing system. Use ``sge`` if it is Sun Oracle Engine / Oracle Grid Engine,
    or ``slurm`` if it is the Slurm Workload Manager.
  - ``q``: default queue name to submit jobs to.
  - ``m``: default amount of memory in GB specified per job
  - ``t``: default time limit in hour specified per job
  - ``p``: default number of processors specified per job
  - ``email``: your email for job notifications, if your system implements them

Note that some systems may be configured not to accept some options. If so, use a value of ``0``
(for integer-type options) or an empty value (for string-type options) to omit that specification.
    
Qjob includes many other options. Run ``qjob -h`` for a summary, and ``qjob -h full`` for a full list.
At any moment, you may come back and edit the ``~/.qjob`` file to change your default settings.

You're all set! Check the :doc:`tutorial` to start using qjob.

    
