package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jakenier/dagpher"
)

var (
	ctx = context.Background()
)

type User struct {
	Uid    int64
	Name   string
	Avatar string
}

type GroupInfo struct {
	Gid      int64
	Name     string
	OwnerUid int64
}

type Payload struct {
	Gid int64

	// load data
	GroupInfo     *GroupInfo
	OwnerInfo     *User
	MemberList    []int64
	UserSetting   string
	MemberRemarks map[int64]string
	MemberInfos   map[int64]*User
}

type Result struct {
	OwnerInfo  *User
	MemberList []*User
}

func GetGroupInfo(ctx context.Context, gid int64) (*GroupInfo, error) {
	time.Sleep(time.Millisecond * 10)
	return &GroupInfo{
		Gid:      gid,
		Name:     fmt.Sprintf("群%v", gid),
		OwnerUid: 1001,
	}, nil
}

func GetOwnerInfo(ctx context.Context, group *GroupInfo) (*User, error) {
	time.Sleep(time.Millisecond * 200)

	return &User{
		Uid:  group.Gid,
		Name: fmt.Sprintf("用户%v", group.Gid),
	}, nil
}

func GetGroupMember(ctx context.Context, group *GroupInfo) ([]int64, error) {
	time.Sleep(time.Millisecond * 20)

	return []int64{1, group.OwnerUid, group.OwnerUid + 1000}, nil
}

func GetMemberRemarks(ctx context.Context, owner *User, memberUids []int64) (map[int64]string, error) {
	time.Sleep(time.Millisecond * 30)

	return dagpher.SliceToMap(memberUids, func(uid int64) (int64, string) {
		if uid == owner.Uid {
			return uid, fmt.Sprintf("群主%v", uid)
		}
		return uid, fmt.Sprintf("成员%v", uid)
	}), nil
}

func GetMemberInfos(ctx context.Context, memberUids []int64) (map[int64]*User, error) {
	time.Sleep(time.Millisecond * 300)
	return dagpher.SliceToMap(memberUids, func(uid int64) (int64, *User) {
		return uid, &User{
			Uid:    uid,
			Name:   fmt.Sprintf("用户%v", uid),
			Avatar: "",
		}
	}), nil
}

type GroupInfoLoader struct{}

func (n *GroupInfoLoader) Name() string {
	return "GroupInfoLoader"
}

func (n *GroupInfoLoader) Depends() []IDepend {
	return nil
}

func (n *GroupInfoLoader) Load(ctx context.Context, c *Payload) error {
	conv, err := GetGroupInfo(ctx, c.Gid)
	if err != nil {
		return err
	}
	c.GroupInfo = conv
	return nil
}

type OwnerInfoLoader struct{}

func (n *OwnerInfoLoader) Name() string {
	return "LoadOwnerInfo"
}

func (n *OwnerInfoLoader) Depends() []IDepend {
	return []IDepend{
		&GroupInfoLoader{},
	}
}

func (n *OwnerInfoLoader) Load(ctx context.Context, c *Payload) error {
	owner, err := GetOwnerInfo(ctx, c.GroupInfo)
	if err != nil {
		return err
	}
	c.OwnerInfo = owner
	return nil
}

type GroupMemberLoader struct{}

func (n *GroupMemberLoader) Name() string {
	return "GroupMemberLoader"
}

func (n *GroupMemberLoader) Depends() []IDepend {
	return []IDepend{
		&GroupInfoLoader{},
	}
}

func (n *GroupMemberLoader) Load(ctx context.Context, c *Payload) error {
	memberList, err := GetGroupMember(ctx, c.GroupInfo)
	if err != nil {
		return err
	}
	c.MemberList = memberList
	return nil
}

type MemberRemarkLoader struct{}

func (n *MemberRemarkLoader) Name() string {
	return "MemberRemarkLoader"
}

func (n *MemberRemarkLoader) Depends() []IDepend {
	return []IDepend{
		&OwnerInfoLoader{},
		&GroupMemberLoader{},
	}
}

func (n *MemberRemarkLoader) Load(ctx context.Context, c *Payload) error {
	remarks, err := GetMemberRemarks(ctx, c.OwnerInfo, c.MemberList)
	if err != nil {
		return err
	}
	c.MemberRemarks = remarks
	return nil
}

type MemberInfoLoader struct{}

func (n *MemberInfoLoader) Name() string {
	return "MemberInfoLoader"
}

func (n *MemberInfoLoader) Depends() []IDepend {
	return []IDepend{
		&GroupMemberLoader{},
	}
}

func (n *MemberInfoLoader) Load(ctx context.Context, c *Payload) error {
	memberInfos, err := GetMemberInfos(ctx, c.MemberList)
	if err != nil {
		return err
	}
	c.MemberInfos = memberInfos
	return nil
}

type AvatarUriProcessor struct {
}

func (n *AvatarUriProcessor) Name() string {
	return "AvatarUriProcessor"
}

func (n *AvatarUriProcessor) Process(ctx context.Context, c *Payload) error {
	time.Sleep(time.Millisecond * 10)

	for _, user := range c.MemberInfos {
		user.Avatar = fmt.Sprintf("avatar/%v", user.Uid)
	}
	return nil
}

type PackAvatarProcessor struct {
}

func (n *PackAvatarProcessor) Name() string {
	return "PackAvatarProcessor"
}

func (n *PackAvatarProcessor) Process(ctx context.Context, c *Payload) error {
	time.Sleep(time.Millisecond * 10)

	for _, user := range c.MemberInfos {
		user.Avatar = "https://" + user.Avatar
	}
	return nil
}

type OwnerAssembler struct {
}

func (a *OwnerAssembler) Name() string {
	return "OwnerAssembler"
}

func (a *OwnerAssembler) Assemble(ctx context.Context, material *Payload, result *Result) error {
	result.OwnerInfo = material.OwnerInfo
	return nil
}

type MemberAssembler struct {
}

func (a *MemberAssembler) Name() string {
	return "MemberAssembler"
}

func (a *MemberAssembler) Assemble(ctx context.Context, material *Payload, result *Result) error {
	result.MemberList = make([]*User, 0, len(material.MemberInfos))
	for _, member := range material.MemberInfos {
		result.MemberList = append(result.MemberList, member)
	}
	return nil
}

func MarshalString(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func TestLoadAssemble(t *testing.T) {
	var (
		ctx     = context.Background()
		payload = &Payload{
			Gid: 1001,
		}
		result = &Result{}
	)
	flow := NewFlow[*Payload, *Result]().
		SetMaxGoNum(10).
		AddGlobalMW(dagpher.LoggerMW(), dagpher.GraphvizMW())
	flow.AddLoader(&GroupInfoLoader{})
	flow.AddLoader(&OwnerInfoLoader{})
	flow.AddLoader(&GroupMemberLoader{})
	flow.AddLoader(&MemberRemarkLoader{})
	flow.AddLoader(&MemberInfoLoader{})

	flow.AddProcessor(&AvatarUriProcessor{})
	flow.AddProcessor(&PackAvatarProcessor{})

	flow.AddAssembler(&OwnerAssembler{})
	flow.AddAssembler(&MemberAssembler{})

	ctx, graph := dagpher.GraphvizBuilder("flow").Build(ctx)
	defer graph.Log(ctx)
	start := time.Now()
	err := flow.Exec(ctx, payload, result)

	fmt.Println(err)
	data := MarshalString(payload)
	fmt.Println(string(data))
	data = MarshalString(result)
	fmt.Println(string(data))
	fmt.Printf("cost: %v\n", time.Since(start))
}
