Troubleshooting
---------------

Queueing systems have many layers of settings. The configuration of your cluster
may require unusual parameters, so that qjob may not work at your first attempt.

The most common problem is that qjob can create job files, but at the time of
submission (with option ``-Q`` active), it crashes: the job submission utility
(``qsub`` or ``sbatch`` depending on the queueing system) did not accept some of
the arguments. Check the error message printed to screen, as it will reveal the culprit.

If there is any option which was not accepted by your system, you can remove them by setting them
to a value of ``0`` or empty string (depending on their type) on your command line or
your ``~/.qjob`` file.

On the other hand, some parameters  may be missing but required by the submission utility.
If this is case, inspect the full list of options by running ``qjob -h full``: most likely,
you will find a qjob option suited to fix the problem. If you can't find any that will add
the right text, you can always resort to option ``-so`` (*submission options*) to add any arbitrary text
when calling the submission utility.

If you have a problem that you can't fix after following these steps, contact the qjob developer
at marco (dot) mariotti (at) ub (dot) edu


