/*
 * gmmethod.h
 *
 *  Created on: Oct 12, 2016
 *      Author: vsam
 */

#ifndef GMMETHOD_H_
#define GMMETHOD_H_


#include "dsnet.h"


class GMLocalHost : public LocalSite
{
protected:
    void initialize() override;

    // handle a stream input to the method
    void handleStreamMessage(int stream, StreamMessage* m) override;

    // handle a coordinator message
    void handleCoordinatorMessage(cPacket*) override;
public:

};


class GMCoordHost : public Coordinator
{
protected:
    void initialize() override;
    void handleSiteMessage(int s, cPacket*) override;

public:

};


#endif /* GMMETHOD_H_ */
