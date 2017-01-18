/*
 * gmmethod.cc
 *
 *  Created on: Oct 12, 2016
 *      Author: vsam
 */


#include "gmmethod.h"

/**************************************
 * Host API
 */

void GMLocalHost::initialize()
{

}


void GMLocalHost::handleStreamMessage(int stream, StreamMessage* m)
{

}

void GMLocalHost::handleCoordinatorMessage(cPacket*)
{

}



void GMCoordHost::initialize()
{

}

void GMCoordHost::handleSiteMessage(int s, cPacket* p)
{

}
