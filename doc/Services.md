# Services
This document gives a brief summary of the services offered by DXRAM which can be called from [dxapps](Applications.md)
running on DXRAM peers.

## ChunkService
For your applications, this is *the* most important service. It provides access to the key-value storage using
*create*, *get*, *put* and *remove* operations. Due to various requirements, there are different types of ChunkServices
available. If you are just getting started, the only services you need are the *ChunkService*.

## ApplicationService
The *ApplicationService* is running on a DXRAM peer and runs DXRAM applications compiled and implemented by the user.
DXRAM applications extend the *AbstractApplication* class and are compiled as separate jar-packages. Further information
can be found in a [dedicated document](Applications.md) about application development.

## BackupService
TODO

## SynchronizationService
TODO

## TemporaryStorageService
TODO
