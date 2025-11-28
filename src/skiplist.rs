use rand::Rng;
use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

const MAX_LEVEL: usize = 16;
const P: f64 = 0.25;

type NodeRef = Arc<Mutex<Node>>;

#[derive(Debug)]
pub struct Level {
    pub forward: Option<NodeRef>,
}

#[derive(Debug)]
pub struct Node {
    pub score: f64,
    pub member: Vec<u8>,
    pub levels: Vec<Level>,
}

impl Node {
    pub fn new(score: f64, member: Vec<u8>, level: usize) -> NodeRef {
        Arc::new(Mutex::new(Self {
            score,
            member,
            levels: (0..level)
                .map(|_| Level { forward: None })
                .collect(),
        }))
    }
}

#[derive(Debug)]
pub struct SkipList {
    pub head: NodeRef,
    pub level: usize,
    pub length: usize,
}

impl SkipList {
    pub fn new() -> Self {
        let head = Node::new(f64::NEG_INFINITY, Vec::new(), MAX_LEVEL);
        Self {
            head,
            level: 1,
            length: 0,
        }
    }

    fn random_level() -> usize {
        let mut lvl = 1;
        let mut rng = rand::thread_rng();
        while rng.gen::<f64>() < P && lvl < MAX_LEVEL {
            lvl += 1;
        }
        lvl
    }

    fn compare(a_score: f64, a_member: &[u8], b_score: f64, b_member: &[u8]) -> Ordering {
        match a_score.partial_cmp(&b_score).unwrap_or(Ordering::Equal) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => a_member.cmp(b_member),
        }
    }

    pub fn insert(&mut self, score: f64, member: Vec<u8>) {
        self.remove_member(&member);

        let mut update: Vec<NodeRef> = Vec::with_capacity(MAX_LEVEL);
        for _ in 0..MAX_LEVEL {
            update.push(self.head.clone());
        }

        let mut current = self.head.clone();

        for lvl in (0..self.level).rev() {
            loop {
                let next_opt = current.lock().unwrap().levels[lvl].forward.clone();
                match next_opt {
                    Some(ref next) => {
                        let nb = next.lock().unwrap();
                        if Self::compare(nb.score, &nb.member, score, &member) == Ordering::Less {
                            current = next.clone();
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }
            update[lvl] = current.clone();
        }

        let new_level = Self::random_level();
        if new_level > self.level {
            for lvl in self.level..new_level {
                update[lvl] = self.head.clone();
            }
            self.level = new_level;
        }

        let new_node = Node::new(score, member, new_level);

        for lvl in 0..new_level {
            let next = update[lvl].lock().unwrap().levels[lvl].forward.clone();
            new_node.lock().unwrap().levels[lvl].forward = next.clone();
            update[lvl].lock().unwrap().levels[lvl].forward = Some(new_node.clone());
        }

        self.length += 1;
    }

    pub fn range_by_score(&self, min: f64, max: f64) -> Vec<Vec<u8>> {
        let mut result = Vec::new();
        let mut current = self.head.clone();

        for lvl in (0..self.level).rev() {
            loop {
                let next_opt = current.lock().unwrap().levels[lvl].forward.clone();
                match next_opt {
                    Some(ref next) => {
                        let nb = next.lock().unwrap();
                        if nb.score < min {
                            current = next.clone();
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }

        let mut current_opt = current.lock().unwrap().levels[0].forward.clone();

        while let Some(node_rc) = current_opt {
            let nb = node_rc.lock().unwrap();
            if nb.score > max {
                break;
            }
            result.push(nb.member.clone());
            current_opt = nb.levels[0].forward.clone();
        }

        result
    }

    pub fn remove_member(&mut self, member: &[u8]) -> bool {
        let mut target: Option<NodeRef> = None;
        let mut current_opt = self.head.lock().unwrap().levels[0].forward.clone();

        while let Some(node_rc) = current_opt.clone() {
            if node_rc.lock().unwrap().member == member {
                target = Some(node_rc.clone());
                break;
            }
            current_opt = node_rc.lock().unwrap().levels[0].forward.clone();
        }

        let target = match target {
            Some(t) => t,
            None => return false,
        };
        
        for lvl in (0..self.level).rev() {
            let mut current = self.head.clone();
            loop {
                let next_opt = current.lock().unwrap().levels[lvl].forward.clone();
                match next_opt {
                    Some(ref next) => {
                        if Arc::ptr_eq(next, &target) {
                            let next_next = next.lock().unwrap().levels[lvl].forward.clone();
                            current.lock().unwrap().levels[lvl].forward = next_next;
                            break;
                        } else {
                            current = next.clone();
                        }
                    }
                    None => break,
                }
            }
        }
        
        self.length -= 1;

        while self.level > 1
            && self.head.lock().unwrap().levels[self.level - 1]
                .forward
                .is_none()
        {
            self.level -= 1;
        }

        true
    }
}