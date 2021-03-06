# A Framework for Data-Intensive Workflow Execution on Multiple Execution Sites
(in progress)
<div><b>Author:</b> Orachun Udokasemsub&nbsp;</div><div>Computer Engineering Department, King Mongkut's University of Technology Thonburi (KMUTT), Thailand</div>
<div><b>Advisors:</b> </div><div>1. Dr. Tiranee Achalakul</div><div>Computer Engineering Department, King Mongkut's University of Technology Thonburi (KMUTT), Thailand
</div><div>2. Dr.Xiaorong Li</div><div>Institute of High Performance Computing (IHPC), Singapore</div><div>
</div>

## Abstract
<div><div>Cloud computing is an emerging technology that combines large amount of&nbsp;<span style="line-height: 1.5em;">computer resources into a virtual place so as to provide the on-demand computing&nbsp;</span><span style="line-height: 1.5em;">facility to users. Scientific simulations are the applications that consist of a huge number&nbsp;</span><span style="line-height: 1.5em;">of the complex computational tasks described by a workflow using directed acyclic&nbsp;</span><span style="line-height: 1.5em;">graphs (DAG). This workflow can be submitted to a cloud system for execution with a&nbsp;</span><span style="line-height: 1.5em;">large amount of computing resources. In order to optimize the performance of the</span></div><div>resource provisioning process on cloud, a workflow scheduling algorithm is needed. In&nbsp;<span style="line-height: 1.5em;">addition, the rapid growth of global data is currently noticed. Thus, many data&nbsp;</span><span style="line-height: 1.5em;">management challenges of scientific workflow executions must be taken into account.</span></div><div>In this research, the framework based on Artificial Bee Colony algorithm to execute&nbsp;<span style="line-height: 1.5em;">data-intensive scientific workflow applications is proposed. The framework includes&nbsp;</span><span style="line-height: 1.5em;">partitioning algorithm, scheduling algorithm, and file management techniques. The</span></div><div>execution will be run on multiple execution sites by partitioning the workflow into each&nbsp;<span style="line-height: 1.5em;">execution site. Moreover, data transferring across execution sites will be managed by a&nbsp;</span><span style="line-height: 1.5em;">technique in the proposed framework in order to reduce additional time caused by data</span></div><div>transferring. Finally, two experiments were designed to be a measure of the&nbsp;<span style="line-height: 1.5em;">performance gained by the proposed framework. It is expected that the proposed&nbsp;</span><span style="line-height: 1.5em;">framework will execute a submitted workflow with better makespan.</span></div></div><div>
</div>

## Dependencies
<ul>
  <li>Java SE</li>
  <li>Mongo DB</li>
  <li>DIFSYS : https://github.com/orachun/difsys</li>
</ul>

## Details
<div>
  The framework consists of two servers: Site Manager and Worker. Site Manager manage the executing site by scheduling submitted workflows and dispatch into its workers, which can be the other site managers or workers. The worker receives tasks dispatched from its site manager, schedules these tasks, and executes on local processor.
  The intermediate files of the submitted workflows are transferred by the designed peer-to-peer method in order to utilize all available network bandwidth within the execution site.
</div>
