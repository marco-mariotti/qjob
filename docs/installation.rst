Installation
------------

We recommend to use the conda package manager to install qjob
(check `this page to install conda / miniconda <https://docs.conda.io/en/latest/miniconda.html>`_).
With conda installed, **run this in a terminal to install qjob**::

    conda install -c mmariotti qjob

You can run this command to check that installation worked correctly, and to see the options of qjob::

    qjob -h

    
Now, before you start using qjob, run this command::

    qjob -setup

This will create a qjob configuration file in your home: ``~/.qjob``.

This file contains all qjob default options for your user.
Open the file in any text editor, and check (and modify if necessary) at least these items:

  - ``sys``: your queueing system. Use ``sge`` if it is Sun Oracle Engine / Oracle Grid Engine,
    or ``slurm`` if it is the Slurm Workload Manager.
  - ``q``: default queue name to submit jobs to.
  - ``m``: default memory in GB requested per job
  - ``t``: default time limit in hour specified per job
  - ``p``: default number of processors specified per job
  - ``email``: your email for job notifications, if your system implements them

Qjob includes many other options. Run ``qjob -h`` for a summary, and ``qjob -h full`` for a full list.
At any moment, you may come back and edit the ``~/.qjob`` file to change your default settings.

You're all set! Check the :doc:`tutorial` to start using qjob.

    
